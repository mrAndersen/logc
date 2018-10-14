<?php declare(strict_types=1);

namespace Logc;

use ClickHouseDB\Client;
use DateTime;
use Exception;

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

        $this->parser = new NginxLogParser();
        $this->clickHouseClient = new Client([
            'username' => $config['clickhouse.username'] ?? 'default',
            'password' => $config['clickhouse.password'] ?? '',
            'host' => $config['clickhouse.host'] ?? '127.0.0.1',
            'port' => $config['clickhouse.port'] ?? '8123'
        ]);
        $this->clickHouseClient->setTimeout(1.5);
        $this->clickHouseClient->setConnectTimeOut(2);
        $this->clickHouseClient->database($config['clickhouse.database'] ?? 'logs');
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
                throw new Exception(sprintf("Clickhouse table \"%s\" not found", $this->clickhouseTable));
            }

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
        $this->write();

        $this->buffer = [];
        $this->sizeBuffer = [];

        $this->lastFlushTime = microtime(true);
    }

    /**
     *
     */
    protected function write()
    {
        $this->clickHouseClient->insert('nginx',
            array_map(function ($node) {
                return [
                    ip2long($node['ip']),
                    (new DateTime($node['date']))->format('Y-m-d H:i:s'),
                    $node['uri'],
                    $node['method'],
                    (int)$node['status'],
                    (int)$node['bytes'],
                ];
            }, $this->buffer), [
                'ip',
                'time',
                'uri',
                'method',
                'status',
                'bytes',
            ]);
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

        while (1) {
            $this->currentTime = microtime(true);

            $bytes = socket_recvfrom($this->socket, $buffer, 4096, 0, $senderIp, $senderPort);
            $parsed = $this->parser->parse($buffer);

            if (!$parsed) {
                continue;
            }

            $this->buffer[] = $parsed;
            $this->sizeBuffer[] = $bytes;

            $sizeCondition = count($this->buffer) >= $this->maxBufferFlushSize;
            $timeCondition = (microtime(true) - $this->lastFlushTime) >= $this->flushPeriod;

            if ($sizeCondition || $timeCondition) {
                $condition = "none";

                if ($sizeCondition) {
                    $condition = "size";
                }

                if ($timeCondition) {
                    $condition = "time";
                }

                $this->stdout(sprintf("Buffer flushed, %d total, %s condition, %d bytes", count($this->buffer), $condition, array_sum($this->sizeBuffer)));
                $this->flush();
            }

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












