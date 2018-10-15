<?php declare(strict_types=1);

namespace Logc\LogParser;


use DateTime;
use Logc\Interfaces\LogParserInterface;

class NginxLogParser implements LogParserInterface
{
    /**
     * @var string
     */
    private $regex = '<(\d+)>(.*)nginx:\s(.*?)\s(.*?)\s\[(.*?)\]\s\"(GET|POST|PUT|HEAD|PATCH|DELETE|UPDATE|OPTIONS|TRACE|PATCH)\s(.*?)\s(.*?)\"\s(\d+)\s(\d+)\s\"(.*?)\"\s\"(.*?)\"$';

    /**
     * @var array
     */
    private $mapping = [
        'unknown1',
        'unknown2',
        'ip',
        'unknown3',
        'date',
        'method',
        'uri',
        'protocol',
        'status',
        'bytes',
        'referer',
        'userAgent'
    ];

    /**
     * NginxLogParser constructor.
     */
    public function __construct()
    {
    }

    /**
     * Map raw log array to clickhouse data
     * @param array $buffer , array of log data
     * @return array
     */
    public function map(array $buffer): array
    {
        return array_map(function ($node) {
            return [
                ip2long($node['ip']),
                (new DateTime($node['date']))->format('Y-m-d H:i:s'),
                $node['uri'],
                $node['method'],
                (int)$node['status'],
                (int)$node['bytes'],
                $node['referer'],
                $node['userAgent']
            ];
        }, $buffer);
    }

    /**
     * @return array
     */
    public function getClickhouseFields(): array
    {
        return [
            'ip',
            'time',
            'uri',
            'method',
            'status',
            'bytes',
            'referer',
            'userAgent',
        ];
    }

    /**
     * @param string $rawMessage
     * @return array
     */
    public function parse(string $rawMessage): array
    {
        preg_match("/" . $this->regex . "/", $rawMessage, $matches);
        unset($matches[0]);

        $matches = array_values($matches);
        $mapped = array_combine($this->mapping, $matches);

        if (!$mapped) {
            return [];
        }

        return $mapped;
    }

    /**
     * @param string $databaseName
     * @param string $tableName
     * @return string
     */
    public function getClickhhouseTableDdl(string $databaseName, string $tableName): string
    {
        return <<<SQL
create table {$databaseName}.{$tableName}
(
	ip UInt32,
	time DateTime,
	date Date default toDate(time),
	uri String,
	method String,
	status UInt16,
	bytes UInt16,
	referer String,
	userAgent String
)
engine = MergeTree(date, (ip, status, time, uri, method, referer, userAgent), 8192);
SQL;

    }
}











