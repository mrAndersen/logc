<?php declare(strict_types=1);


namespace Logc\Exception;


use DateTime;
use Exception;
use Throwable;

class ParseException extends Exception
{
    /**
     * ParseException constructor.
     * @param string $message
     * @param int $code
     * @param Throwable|null $previous
     * @throws Exception
     */
    public function __construct($message = "", $code = 0, Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);

        if (!file_exists('error.log')) {
            file_put_contents('error.log', (new DateTime())->format('d.m.Y') . "\n");
        }

        file_put_contents('error.log', $message . "\n", FILE_APPEND);
    }


}