<?php declare(strict_types=1);

namespace Logc;

use ClickHouseDB\Client;
use DateTime;
use Exception;
use Logc\LogParser\NginxLogParser;

class LogcUdpServer
{
    const VERBOSITY_NONE = 0;
    const VERBOSITY_DEBUG = 1;

    /**
     * @var string
     */
    private $address = "0.0.0.0";

    /**
     * @var int
     */
    private $port = 914;

    /**
     * @var resource
     */
    private $socket;

    /**
     * @var NginxLogParser
     */
    private $parser;

    /**
     * @var Client
     */
    private $clickHouseClient;

    /**
     * @var int
     */
    private $startTime = 0;

    /**
     * @var int
     */
    private $currentTime = 0;

    /**
     * @var int
     */
    private $lastFlushTime = 0;

    /**
     * @var int
     */
    private $flushPeriod = 10;

    /**
     * @var int
     */
    private $maxBufferFlushSize = 10;

    /**
     * @var array
     */
    private $buffer = [];

    /**
     * @var array
     */
    private $sizeBuffer = [];

    /**
     * @var int
     */
    private $verbosity = self::VERBOSITY_DEBUG;

    /**
     * @var string
     */
    private $clickhouseTable = 'nginx';

    /**
     * @var string
     */
    private $clickhouseDtabase = "logs";

    /**
     * @var float
     */
    private $lastFlushDuration = 0;

    /**
     * UdpServer constructor.
     * @param string $configPath
     * @throws Exception
     */
    public function __construct(string $configPath)
    {
        $config = parse_ini_file($configPath);

        if (!$config) {
            throw new Exception(sprintf("Configuration file %s can't be parsed", $configPath));
        }

        $this->address = $config['bind'] ?? '0.0.0.0';
        $this->port = $config['port'] ?? '914';
        $this->maxBufferFlushSize = $config['buffer.max_flush_size'] ?? 5000;
        $this->flushPeriod = $config['buffer.max_flush_period'] ?? 10;

        $this->clickhouseTable = $config['clickhouse.table'] ?? 'nginx';
        $this->clickhouseDtabase = $config['clickhouse.database'] ?? 'logs';

        $this->verbosity = $config['verbosity'] ?? self::VERBOSITY_NONE;

        if (!($this->socket = socket_create(AF_INET, SOCK_DGRAM, 0))) {
            $errorCode = socket_last_error();
            $errorMessage = socket_strerror($errorCode);

            throw new Exception("Couldn't create socket: [$errorCode] $errorMessage");
        }

        if (!socket_bind($this->socket, $this->address, (int)$this->port)) {
            $errorCode = socket_last_error();
            $errorMessage = socket_strerror($errorCode);

            throw new Exception("Could not bind to {$this->address} [$errorCode] $errorMessage");
        }

        socket_set_nonblock($this->socket);

        $this->parser = new NginxLogParser();
        $this->clickHouseClient = new Client([
            'username' => $config['clickhouse.username'] ?? 'default',
            'password' => $config['clickhouse.password'] ?? '',
            'host' => $config['clickhouse.host'] ?? '127.0.0.1',
            'port' => $config['clickhouse.port'] ?? '8123'
        ]);
        $this->clickHouseClient->setTimeout(1.5);
        $this->clickHouseClient->setConnectTimeOut(2);
        $this->clickHouseClient->database(
            $this->clickhouseDtabase
        );
    }

    /**
     * @param string $message
     */
    protected function stdout(string $message)
    {
        $d = new DateTime();
        echo(sprintf("[%s] {$message}\n", $d->format('c')));
    }

    /**
     *
     */
    protected function pingClickhouse()
    {
        try {
            $size = $this->clickHouseClient->tableSize($this->clickhouseTable);

            if (!$size) {
                $this->stdout(sprintf("Clickhouse table \"%s\" not found", $this->clickhouseTable));

                $this->clickHouseClient->write(
                    $this->parser->getClickhhouseTableDdl(
                        $this->clickhouseDtabase,
                        $this->clickhouseTable
                    )
                );

                $this->stdout(sprintf("Created table \"%s\"", $this->clickhouseTable));
            }

            $size = $this->clickHouseClient->tableSize($this->clickhouseTable);
            $this->stdout(sprintf(
                "Connected to clickhouse at %s:%d, table = %s, size=%s",
                $this->clickHouseClient->getConnectHost(),
                $this->clickHouseClient->getConnectPort(),
                $this->clickhouseTable,
                $size['size']
            ));
        } catch (Exception $exception) {
            $this->stdout($exception->getMessage());
            $this->close();
        }
    }

    /**
     *
     */
    protected function flush()
    {
        $start = microtime(true);
        $this->write();

        $this->buffer = [];
        $this->sizeBuffer = [];

        $this->lastFlushTime = microtime(true);
        $this->lastFlushDuration = microtime(true) - $start;
    }

    /**
     *
     */
    protected function write()
    {
        if (!$this->buffer) {
            return;
        }

        $this->clickHouseClient->insert(
            $this->clickhouseTable,
            $this->parser->map($this->buffer),
            $this->parser->getClickhouseFields()
        );
    }

    /**
     *
     */
    public function run()
    {
        $this->stdout(sprintf("Started logc at {$this->address}:{$this->port}, verbosity %s", $this->verbosity));
        $this->pingClickhouse();

        $this->startTime = microtime(true);
        $this->lastFlushTime = $this->startTime;
        $sleepInterval = 1000000;

        while (1) {
            $this->currentTime = microtime(true);

            $sizeCondition = count($this->buffer) >= $this->maxBufferFlushSize;
            $timeCondition = ($this->currentTime - $this->lastFlushTime) >= $this->flushPeriod;

            if ($sizeCondition || $timeCondition) {
                $condition = "none";

                if ($sizeCondition) {
                    $condition = "size";
                }

                if ($timeCondition) {
                    $condition = "time";
                }

                $buffSize = array_sum($this->sizeBuffer);
                $buffCount = count($this->buffer);

                $this->flush();
                $this->stdout(sprintf(
                    "Buffer flushed, %d total, %s condition, %d bytes in %d ms, %d memory",
                    $buffCount,
                    $condition,
                    $buffSize,
                    round($this->lastFlushDuration * 1000, 0),
                    memory_get_usage(true)
                ));
            }

            $bytes = socket_recvfrom($this->socket, $buffer, 4096, 0, $senderIp, $senderPort);

            if (!$bytes) {
                if ($this->verbosity == self::VERBOSITY_DEBUG) {
                    $this->stdout(sprintf("No data, sleeping %dms", $sleepInterval / 1000));
                }

                usleep($sleepInterval);
                continue;
            }

            $parsed = $this->parser->parse($buffer);

            if (!$parsed) {
                $this->stdout(sprintf("Unable to parse message %s", $buffer));
                continue;
            }

            $this->buffer[] = $parsed;
            $this->sizeBuffer[] = $bytes;

            if ($this->verbosity == self::VERBOSITY_DEBUG) {
                $this->stdout($parsed['uri']);
            }
        }
    }

    /**
     *
     */
    public function close()
    {
        socket_close($this->socket);
        die();
    }
}












