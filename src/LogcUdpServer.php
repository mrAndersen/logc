<?php declare(strict_types=1);

namespace Logc;

use ClickHouseDB\Client;
use DateTime;
use Exception;

class LogcUdpServer
{
    const VERBOSITY_DEBUG = 1;
    const VERBOSITY_NONE = 2;

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
    private $maxBufferFlushSize = 10000;

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
     * UdpServer constructor.
     * @throws Exception
     */
    public function __construct()
    {
        if (!($this->socket = socket_create(AF_INET, SOCK_DGRAM, 0))) {
            $errorCode = socket_last_error();
            $errorMessage = socket_strerror($errorCode);

            throw new Exception("Couldn't create socket: [$errorCode] $errorMessage");
        }


        if (!socket_bind($this->socket, $this->address, $this->port)) {
            $errorCode = socket_last_error();
            $errorMessage = socket_strerror($errorCode);

            throw new Exception("Could not bind to {$this->address} [$errorCode] $errorMessage");
        }

        $this->parser = new NginxLogParser();
        $this->clickHouseClient = new Client([
            'username' => 'default',
            'password' => '',
            'host' => 'localhost',
            'port' => '12123'
        ]);
        $this->clickHouseClient->setTimeout(1.5);
        $this->clickHouseClient->setConnectTimeOut(2);
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
        $tables = $this->clickHouseClient->showTables();
    }

    /**
     *
     */
    protected function flush()
    {
        $this->buffer = [];
        $this->sizeBuffer = [];

        $this->lastFlushTime = microtime(true);
    }

    protected function write()
    {
        


    }

    /**
     *
     */
    public function run()
    {
        $this->stdout(sprintf("Started logc at {$this->address}:{$this->port}, verbosity %s", $this->verbosity));

        $this->pingClickhouse();
        $this->stdout(sprintf(
            "Connected to clickhouse at %s:%d",
            $this->clickHouseClient->getConnectHost(),
            $this->clickHouseClient->getConnectPort()
        ));

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
        $this->flush();
        socket_close($this->socket);
    }
}












