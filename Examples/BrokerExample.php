<?php

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * Log to STDOUT
 */
$logger = new \Monolog\Logger('broker');
$streamHandler = new \Monolog\Handler\StreamHandler('php://stdout');
$streamHandler->setFormatter(new \Monolog\Formatter\LineFormatter(null, 'Y-m-d H:i:s:u'));
$logger->pushHandler($streamHandler);

$broker = new \Iris\Broker();
$broker
    ->setLogger($logger)
    ->bind('tcp://*:5555')
    ->listen();
