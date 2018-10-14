<?php declare(strict_types=1);


namespace Logc\Interfaces;


interface LogParserInterface
{
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

}