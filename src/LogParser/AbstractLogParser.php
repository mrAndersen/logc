<?php declare(strict_types=1);


namespace Logc\LogParser;


use ClickHouseDB\Client;

abstract class AbstractLogParser
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var array
     */
    protected $settings = [];

    /**
     * @var string
     */
    protected $name = "some_output";

    /**
     * AbstractLogParser constructor.
     * @param array $settings
     */
    public function __construct(array $settings)
    {
        $this->client = new Client([
            'username' => $settings['username'] ?? 'default',
            'password' => $settings['password'] ?? '',
            'host' => $settings['host'] ?? '127.0.0.1',
            'port' => $settings['port'] ?? '8123'
        ]);
        $this->client->setTimeout($settings['timeout']['write']);
        $this->client->setConnectTimeOut($settings['timeout']['connection']);
        $this->client->database($settings['database']);

        $this->settings = $settings;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @param string $name
     */
    public function setName(string $name): void
    {
        $this->name = $name;
    }

    /**
     * @return Client
     */
    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * @return array
     */
    public function getSettings(): array
    {
        return $this->settings;
    }
}