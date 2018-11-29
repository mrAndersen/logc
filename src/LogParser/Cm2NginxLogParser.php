<?php declare(strict_types=1);


namespace Logc\LogParser;


use DateTime;
use DateTimeZone;
use Exception;
use Logc\Exception\ParseException;
use Logc\Interfaces\LogParserInterface;

class Cm2NginxLogParser extends AbstractLogParser implements LogParserInterface
{
    /**
     * @var string
     */
    private $regex = '<(\d+)>(.*)nginx:\s<time=(.*)\|url=(.*)\|status=(\d+)\|referer=(.*)\|bytes=(\d+)\|cache=(.*)\|method=(GET|POST|PUT|HEAD|PATCH|DELETE|UPDATE|OPTIONS|TRACE|PATCH)\|body=(.*)\|request_time=(.*)>$';

    /**
     * @var array
     */
    private $mapping = [
        'unknown1',
        'unknown2',
        'date',
        'uri',
        'status',
        'referer',
        'bytes',
        'cache',
        'method',
        'body',
        'requestTime'
    ];

    /**
     * NginxLogParser constructor.
     * @param array $settings
     */
    public function __construct(array $settings)
    {
        parent::__construct($settings);
    }

    /**
     * @param array $buffer
     * @return array
     */
    public function map(array $buffer): array
    {
        return array_map(function ($node) {
            return [
                (new DateTime($node['date']))->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s'),
                $node['uri'],
                (int)$node['status'],
                $node['referer'],
                (int)$node['bytes'],
                $node['cache'],
                $node['method'],
                $node['body'],
                (float)$node['requestTime'],
            ];
        }, $buffer);
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return "nginx";
    }

    /**
     * @param string $rawMessage
     * @return array
     * @throws ParseException
     */
    public function parse(string $rawMessage): array
    {
        try {
            preg_match("/" . $this->regex . "/", $rawMessage, $matches);
            unset($matches[0]);

            $matches = array_values($matches);
            $mapped = array_combine($this->mapping, $matches);

            if (count($matches) !== count($this->mapping)) {
                throw new Exception("Error mapping regex");
            }

            if (!$mapped) {
                return [];
            }

            return $mapped;
        } catch (Exception $exception) {
            throw new ParseException("Error while parsing message %s, %s\n", $rawMessage, $exception->getMessage());
        }
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
    time DateTime,
    date Date default toDate(time),
    uri String,
    status UInt16,
    referer String,
    bytes UInt16,
    cache String,
    method String,
    body String,
    requestTime Float32
)
engine = MergeTree(date, (status, time, uri, method), 8192);
SQL;
    }

    /**
     * @return array
     */
    public function getClickhouseFields(): array
    {
        return [
            'time',
            'uri',
            'status',
            'referer',
            'bytes',
            'cache',
            'method',
            'body',
            'requestTime'
        ];
    }


}