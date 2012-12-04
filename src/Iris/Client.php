<?php

namespace Iris;

use Iris\Mdp;
use Iris\Message;

/**
 * A client connects to a broker and issues requests
 */
class Client
{
    protected $socket;
    protected $timeout = 2500;

    public function __construct()
    {
    }

    public function connect($dsn)
    {
        if (null !== $this->socket) {
            unset($this->socket);
        }

        $context = new \ZMQContext();
        $this->socket = new \ZMQSocket($context, \ZMQ::SOCKET_DEALER);
        $this->socket->setSockOpt(\ZMQ::SOCKOPT_LINGER, 0);
        $this->socket->connect($dsn);
    }

    public function send($service, $message)
    {
        $message->push($service);
        $message->push(Mdp::CLIENT);
        $message->push('');

        $message->setSocket($this->socket)->send();
    }

    public function recv()
    {
        $read = $write = array();
        $poll = new \ZMQPoll();
        $poll->add($this->socket, \ZMQ::POLL_IN);
        $events = $poll->poll($read, $write, $this->timeout);
        if ($events) {
            $message = new Message($this->socket);
            $message->recv();
            $message->pop();
            $header = $message->pop();
            $reply_service = $message->pop();
            return $message;
        }
    }
}
