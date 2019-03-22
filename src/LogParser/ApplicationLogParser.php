<?php declare(strict_types=1);


namespace Logc\LogParser;


use DateTime;
use DateTimeZone;
use Exception;
use Logc\Interfaces\LogParserInterface;

class ApplicationLogParser extends AbstractLogParser implements LogParserInterface
{
    /**
     * @param string $rawMessage
     * @return array
     * @throws Exception
     */
    public function parse(string $rawMessage): array
    {
        $rawMessage = str_replace("<42> cm2_application ", "", $rawMessage);
        $rawMessage = json_decode($rawMessage, true);

        if (!$rawMessage) {
            return [];
        }

        $result = [];

        $base = [
            (new DateTime(
                $rawMessage['datetime']['date'],
                new DateTimeZone($rawMessage['datetime']['timezone'])
            ))->format('Y-m-d H:i:s'),
            (new DateTime(
                $rawMessage['datetime']['date'],
                new DateTimeZone($rawMessage['datetime']['timezone'])
            ))->format('Y-m-d'),
            $rawMessage['channel'],
            $rawMessage['message'],
            $rawMessage['level']
        ];

        $result = array_merge($result, $base);

        if (isset($rawMessage['context']['custom']) && $rawMessage['context']['custom'] === true) {
            $customMessage = $rawMessage['context'];

            if (count($customMessage['stringProperties']) > 6 || count($customMessage['floatProperties']) > 6) {
                return [];
            }

            ksort($customMessage['stringProperties']);
            ksort($customMessage['floatProperties']);

            $result = array_merge($result, [$customMessage['type']], $customMessage['stringProperties'], $customMessage['floatProperties'], [[]]);
            return $result;
        }

        $result = array_merge($result, [
            -1,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            []
        ]);

        return $result;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return "cm2_application";
    }
}