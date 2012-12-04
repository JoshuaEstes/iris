<?php

require_once __DIR__ . '/../vendor/autoload.php';

/**
 * Create and connect client to broker
 */
$client = new \Iris\Client();
$client
    ->connect('tcp://127.0.0.1:5555');

/**
 * Send a few messages
 */
for ($i = 0; $i < 10; $i++) {
    $message = new \Iris\Message();
    $message->setBody('Hello World');
    $client->send('echo', $message);
    $reply = $client->recv();
    printf("Received Reply [%s]\n", $reply->getBody());
}

/**
 * Send a lot of messages and get the responses later
 */
$totalRequests = 1000;
for ($i = 0; $i < $totalRequests; $i++) {
    $message = new \Iris\Message();
    $message->setBody('Hello World');
    $client->send('echo', $message);
}

for ($i = 0; $i < $totalRequests; $i++) {
    $response = $client->recv();
    if (null === $response) {
        break;
    }
}

printf("Received [%d] respones\n", $i);
