<?php

namespace Iris;

use Iris\Mdp;
use Iris\Message;

class Worker
{
    protected $logger;

    protected $heartbeat_at;
    protected $heartbeat_liveness = 5;
    protected $heartbeat_interval = 2500;
    protected $reconnect          = 2500;
    protected $service;
    protected $dsn;

    public function __construct()
    {
    }

    public function setLogger($logger)
    {
        $this->logger = $logger;
        return $this;
    }

    public function setService($service)
    {
        $this->service = $service;
        return $this;
    }

    public function connect($dsn)
    {
        $this->dsn = $dsn;
        $context = new \ZMQContext();
        $this->socket = new \ZMQSocket($context, \ZMQ::SOCKET_DEALER);
        $this->socket->connect($this->dsn);
        $this->sendToBroker(Mdp::READY, $this->service);
        $this->liveness = $this->heartbeat_liveness;
        $this->heartbeat_at = microtime(true) + ($this->heartbeat_interval / 1000);
    }

    public function sendToBroker($command, $option = null, $message = null)
    {
        if (null === $message) {
            $message = new Message();
        }

        if ($option) {
            $message->push($option);
        }

        $this->logger->debug('Sending message to broker.', array(
            'command' => $command,
            'option'  => $option,
        ));

        $message->push($command);
        $message->push(Mdp::WORKER);
        $message->push("");

        $message->setSocket($this->socket)->send();
    }

    public function recv($reply = null)
    {
        if ($reply) {
            $reply->wrap($this->reply_to);
            $this->sendToBroker(Mdp::REPLY, null, $reply);
        }
        $this->expect_reply = true;

        $read = $write = array();
        while(true) {
            $poll = new \ZMQPoll();
            $poll->add($this->socket, \ZMQ::POLL_IN);
            $events = $poll->poll($read, $write, 2500);

            if ($events) {
                $message = new Message($this->socket);
                $message->recv();

                $this->liveness = $this->heartbeat_liveness;
                $message->pop();
                $header = $message->pop();
                $command = $message->pop();
                switch($command) {
                case(Mdp::REQUEST):
                    $this->reply_to = $message->unwrap();
                    $this->logger->debug('Receiving Request', array(
                        'body' => $message->getBody(),
                    ));
                    return $message;
                    break;
                case(Mdp::HEARTBEAT):
                    break;
                case(Mdp::DISCONNECT):
                    $this->connect($this->dsn);
                    break;
                default:
                    $this->logger->alert('Unknow command', array(
                        'command' => $command,
                    ));
                    break;
                }

            } elseif(--$this->liveness == 0) {
                usleep(2500*1000);
                $this->connect($this->dsn);
            }

            if (microtime(true) > $this->heartbeat_at) {
                $this->sendToBroker(Mdp::HEARTBEAT);
                $this->heartbeat_at = microtime(true) + (2500/1000);
            }
        }
    }

    public function send($command, $message)
    {
    }

}
