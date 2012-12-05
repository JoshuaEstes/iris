<?php

require_once __DIR__ . '/../vendor/autoload.php';

$logger = new \Monolog\Logger('worker');
$streamHandler = new \Monolog\Handler\StreamHandler('php://stdout');
$streamHandler->setFormatter(new \Monolog\Formatter\LineFormatter(null, 'Y-m-d H:i:s:u'));
$logger->pushHandler($streamHandler);

$worker = new \Iris\Worker();
$worker->setLogger($logger);
$worker->setService('echo');
$worker->connect('tcp://127.0.0.1:5555');

$reply = null;
while (true) {
    $request = $worker->recv($reply);
    $reply   = $request;
}
