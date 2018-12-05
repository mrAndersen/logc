<?php declare(strict_types=1);

namespace Logc\LogParser;


use DateTime;
use DateTimeZone;
use Exception;
use Logc\Exception\ParseException;
use Logc\Interfaces\LogParserInterface;

class NginxLogParser extends AbstractLogParser implements LogParserInterface
{
    /**
     * @var string
     */
    private $regex = '<(\d+)>(.*)nginx:\s(.*?)\s\[(.*?)\]\s\"(GET|POST|PUT|HEAD|PATCH|DELETE|UPDATE|OPTIONS|TRACE|PATCH)\s(.*?)\s(.*?)\"\s(\d+)\s(\d+)\s\"(.*?)\"\s\"(.*?)\"$';

    /**
     * @var array
     */
    private $mapping = [
        'unknown1',
        'unknown2',
        'ip',
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
     * @param array $settings
     */
    public function __construct(array $settings)
    {
        parent::__construct($settings);
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

            return [
                ip2long($mapped['ip']),
                (new DateTime($mapped['date']))->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s'),
                (new DateTime($mapped['date']))->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d'),
                $mapped['uri'],
                $mapped['method'],
                $mapped['protocol'],
                (int)$mapped['status'],
                (int)$mapped['bytes'],
                $mapped['referer'],
                $mapped['userAgent']
            ];
        } catch (Exception $exception) {
            throw new ParseException(
                sprintf(
                    "Error while parsing message %s, matches %s, %s\n",
                    $rawMessage,
                    implode(',', $matches),
                    $exception->getMessage()
                )
            );
        }
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return "nginx";
    }
}











