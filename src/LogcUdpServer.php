<?php declare(strict_types=1);

namespace Logc;

use DateTime;
use Exception;
use Logc\Interfaces\LogParserInterface;
use Symfony\Component\Yaml\Yaml;
use Throwable;

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
     * @var LogParserInterface[]
     */
    private $parsers = [];

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
     * @var float
     */
    private $lastFlushDuration = 0;

    /**
     * @var
     */
    private $currentWriteParser;

    /**
     * UdpServer constructor.
     * @param string $configPath
     * @throws Exception
     */
    public function __construct(string $configPath)
    {
        $this->prepareConfig($configPath);
        $this->prepareSocket();
    }

    /**
     * @param string $configPath
     * @throws Exception
     */
    protected function prepareConfig(string $configPath)
    {
        $config = Yaml::parseFile($configPath);

        if (!$config) {
            throw new Exception(sprintf("Configuration file %s can't be parsed", $configPath));
        }

        $this->address = $config['logc']['bind'] ?? '0.0.0.0';
        $this->port = $config['logc']['port'] ?? '914';
        $this->maxBufferFlushSize = $config['logc']['buffer']['max_flush_size'] ?? 5000;
        $this->flushPeriod = $config['logc']['buffer']['max_flush_period'] ?? 10;
        $this->verbosity = $config['logc']['verbosity'] ?? self::VERBOSITY_NONE;

        foreach ($config['outputs'] as $name => $output) {
            /** @var LogParserInterface $parser */
            $parser = new $output['parser']($output);
            $parser->setName($name);
            $this->parsers[] = $parser;
        }
    }

    /**
     * @throws Exception
     */
    protected function prepareSocket()
    {
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
    }

    /**
     *
     * @throws Exception
     */
    public function run()
    {
        $this->stdout(sprintf("Started logc at {$this->address}:{$this->port}, verbosity %s", $this->verbosity));
        $this->pingClickhouse();

        $this->startTime = microtime(true);
        $this->lastFlushTime = $this->startTime;
        $sleepInterval = 1 * 1000;

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

                $buffCount = 0;
                foreach ($this->buffer as $outputs) {
                    $buffCount += count($outputs);
                }

                $this->flush();
                $this->stdout(sprintf(
                    "Buffer flushed, %d total, %s condition, %d bytes in %d ms, %.2f MB memory usage",
                    $buffCount,
                    $condition,
                    $buffSize,
                    round($this->lastFlushDuration * 1000, 0),
                    memory_get_usage(true) / 1024 / 1024
                ));
            }

            try {
                $bytes = socket_recvfrom($this->socket, $message, 10240, 0, $senderIp, $senderPort);
            } catch (Throwable $throwable) {
                $bytes = false;
                $this->stdout(sprintf("Error while socket_recvfrom on message %s", $message));
            }

            if (!$bytes) {
                usleep($sleepInterval);
                continue;
            }

            if ($this->verbosity == self::VERBOSITY_DEBUG) {
                $this->stdout($message);
            }

            foreach ($this->parsers as $parser) {
                if (strpos($message, $parser->getName()) !== false) {

                    try {
                        $parsed = $parser->parse($message);
                    } catch (Throwable $throwable) {
                        $parsed = false;
                    }

                    if (!$parsed) {
                        $this->stdout(sprintf("Unable to parse message %s", $message));
                        continue;
                    }

                    $this->buffer[$parser->getName()][] = $parsed;
                    $this->sizeBuffer[] = $bytes;

                    if ($this->verbosity == self::VERBOSITY_DEBUG) {
                        $this->stdout(sprintf(
                            "Parsed %s from %s",
                            $parsed['uri'],
                            $parser->getName()
                        ));
                    }
                }
            }
        }
    }

    /**
     * @param string $message
     * @throws Exception
     */
    protected function stdout(string $message)
    {
        $d = new DateTime();
        echo(sprintf("[%s] %s\n", $d->format('c'), $message));
    }

    /**
     *
     * @throws Exception
     */
    protected function pingClickhouse()
    {
        try {
            foreach ($this->parsers as $parser) {
                $size = $parser->getClient()->tableSize($parser->getSettings()['table']);

                if (!$size) {
                    $this->stdout(
                        sprintf(
                            "Clickhouse table \"%s\" not found on provider %s",
                            $parser->getSettings()['table'],
                            $parser->getName()
                        )
                    );

                    $parser->getClient()->write(
                        $this->createDdl($parser)
                    );

                    $this->stdout(
                        sprintf(
                            "Created table \"%s\" on provider %s",
                            $parser->getSettings()['table'],
                            $parser->getName()
                        )
                    );
                }

                $size = $parser->getClient()->tableSize($parser->getSettings()['table']);

                $this->stdout(sprintf(
                    "Connected to clickhouse at %s:%d, table = %s, size=%s",
                    $parser->getClient()->getConnectHost(),
                    $parser->getClient()->getConnectPort(),
                    $parser->getSettings()['table'],
                    $size['size']
                ));
            }
        } catch (Exception $exception) {
            $this->stdout($exception->getMessage());
            $this->close();
        }
    }

    /**
     * @param LogParserInterface $parser
     * @return string
     * @throws Exception
     */
    protected function createDdl(LogParserInterface $parser)
    {
        $schema = $parser->getSettings()['schema'] ?? null;
        $databaseName = $parser->getSettings()['database'] ?? null;
        $tableName = $parser->getSettings()['table'] ?? null;
        $engine = $parser->getSettings()['engine'] ?? null;

        if (!$schema || !$databaseName || !$tableName || !$engine) {
            throw new Exception(sprintf("Unable to create table for %s output, no table, database or schema provided", $parser->getName()));
        }

        $temp = [];

        foreach ($schema as $field => $type) {
            $temp[] = "{$field} {$type}";
        }

        $tableDdl = implode(',', $temp);
        $sql = "create table {$databaseName}.{$tableName} ({$tableDdl}) engine = {$engine}";
        return $sql;
    }

    /**
     *
     */
    public function close()
    {
        socket_close($this->socket);
        die();
    }

    /**
     *
     * @throws Exception
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
     * @param int $tries
     * @throws Exception
     */
    protected function write(int &$tries = 0)
    {
        $maxTries = 3;
        $tryTtl = 2;

        if (!$this->buffer) {
            return;
        }

        try {
            foreach ($this->parsers as $parser) {
                $this->currentWriteParser = $parser;

                if (!$this->buffer[$parser->getName()]) {
                    continue;
                }

                $parser
                    ->getClient()
                    ->insert(
                        $parser->getSettings()['table'],
                        $this->buffer[$parser->getName()],
                        $parser->getClickhouseFields()
                    );
            }
        } catch (Exception $exception) {
            if ($tries < $maxTries) {
                $this->stdout(
                    sprintf(
                        "Error during clickhouse flush on provider %s, will retry in %d seconds",
                        $this->currentWriteParser->getName(),
                        $tryTtl
                    )
                );

                $this->stdout(
                    $exception->getMessage()
                );

                sleep($tryTtl);
                $tries++;

                $this->write($tries);
            } else {
                $this->stdout(
                    sprintf(
                        "Fatal error, unable to flush to clickhouse on provider %s during %d tries",
                        $this->currentWriteParser->getName(),
                        $maxTries
                    ));
                $this->close();
            }
        }
    }

    /**
     * @param string $message
     * @throws Exception
     */
    protected function appendFileLog(string $message)
    {
        $logFile = "../error.log";

        if (!file_exists($logFile)) {
            file_put_contents($logFile, (new DateTime())->format('d.m.Y') . "\n");
        }

        file_put_contents($logFile, $message . "\n", FILE_APPEND);
    }
}












