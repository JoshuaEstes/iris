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
    if (!empty($reply)) {
        printf("Received Reply [%s]\n", $reply->getBody());
    } else {
        printf("There was no response in a timely manner.\n");
    }
}

/**
 * Send a lot of messages and get the responses later
 * 
 * NOTE: Don't set this to high as if seems that running on localhost
 *       that 0mq processes faster than is able to spit out to stdout
 */
$totalRequests = 100;
printf("Sending out [%d] Requests\n", $totalRequests);
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
