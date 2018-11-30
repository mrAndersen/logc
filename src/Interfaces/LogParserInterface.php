<?php declare(strict_types=1);


namespace Logc\Interfaces;


use ClickHouseDB\Client;

interface LogParserInterface
{
    /**
     * LogParserInterface constructor.
     * @param array $settings
     */
    public function __construct(array $settings);

    /**
     * @param string $rawMessage
     * @return array
     */
    public function parse(string $rawMessage): array;

    /**
     * @return array
     */
    public function getClickhouseFields(): array;

    /**
     * @return array
     */
    public function getSettings(): array;

    /**
     * @return Client
     */
    public function getClient(): Client;

    /**
     * @return string
     */
    public function getName(): string;

    /**
     * @param string $name
     * @return mixed
     */
    public function setName(string $name);

}