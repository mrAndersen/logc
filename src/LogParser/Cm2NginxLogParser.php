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
    private $regexMapping = [
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
            $mapped = array_combine($this->regexMapping, $matches);

            if (count($matches) !== count($this->regexMapping)) {
                throw new Exception("Error mapping regex");
            }

            if (!$mapped) {
                return [];
            }

            return [
                (new DateTime($mapped['date']))->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s'),
                (new DateTime($mapped['date']))->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d'),
                $mapped['uri'],
                (int)$mapped['status'],
                $mapped['referer'],
                (int)$mapped['bytes'],
                $mapped['cache'],
                $mapped['method'],
                $mapped['body'],
                (float)$mapped['requestTime'],
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


}