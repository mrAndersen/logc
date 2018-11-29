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
     * @param string $databaseName
     * @param string $tableName
     * @return string
     */
    public function getClickhhouseTableDdl(string $databaseName, string $tableName): string;

    /**
     * @return array
     */
    public function getClickhouseFields(): array;

    /**
     * @param array $buffer
     * @return array
     */
    public function map(array $buffer): array;

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