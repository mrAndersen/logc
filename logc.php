<?php

use Logc\LogcUdpServer;

require 'vendor/autoload.php';
set_time_limit(0);

$configIni = __DIR__ . "/logc.ini";

foreach ($argv as $arg) {
    if ($arg === "--configuration") {
        $configIni = explode("=", $arg)[1];
    }
}

$server = new LogcUdpServer($configIni);
$server->run();