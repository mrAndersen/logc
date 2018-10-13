<?php

use Logc\LogcUdpServer;

require 'vendor/autoload.php';

set_time_limit(0);

$server = new LogcUdpServer();
$server->run();