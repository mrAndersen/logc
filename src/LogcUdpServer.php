<?php declare(strict_types=1);

namespace Logc;

use DateTime;
use Exception;
use Logc\Interfaces\LogParserInterface;
use Symfony\Component\Yaml\Yaml;

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
                    "Buffer flushed, %d total, %s condition, %d bytes in %d ms, %f memory",
                    $buffCount,
                    $condition,
                    $buffSize,
                    round($this->lastFlushDuration * 1000, 0),
                    round(memory_get_usage(true) / 1024 / 1024, 2)
                ));
            }

            $bytes = socket_recvfrom($this->socket, $message, 4096, 0, $senderIp, $senderPort);

            if (!$bytes) {
                if ($this->verbosity == self::VERBOSITY_DEBUG) {
                    $this->stdout(sprintf("No data, sleeping %dms", $sleepInterval / 1000));
                }

                usleep($sleepInterval);
                continue;
            }

            if ($this->verbosity == self::VERBOSITY_DEBUG) {
                $this->stdout($message);
            }

            foreach ($this->parsers as $parser) {
                if (strpos($message, $parser->getName()) !== false) {
                    $parsed = $parser->parse($message);

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
        echo(sprintf("[%s] {$message}\n", $d->format('c')));
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
                        $parser->getClickhhouseTableDdl(
                            $parser->getSettings()['database'],
                            $parser->getSettings()['table']
                        )
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

                $parser
                    ->getClient()
                    ->insert(
                        $parser->getSettings()['table'],
                        $parser->map($this->buffer[$parser->getName()]),
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
}












