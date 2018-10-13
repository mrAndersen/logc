<?php declare(strict_types=1);

namespace Logc;


class NginxLogParser
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
        'unknown3',
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
     */
    public function __construct()
    {
    }

    /**
     * @param string $rawMessage
     * @return array
     */
    public function parse(string $rawMessage)
    {
        preg_match("/" . $this->regex . "/", $rawMessage, $matches);
        unset($matches[0]);
        $matches = array_values($matches);

        $mapped = array_combine($this->mapping, $matches);
        return $mapped;
    }
}











