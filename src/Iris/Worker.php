<?php

namespace Iris;

use Iris\Mdp;
use Iris\Message;
use Monolog\Logger;

/**
 * @author Joshua Estes
 */
class Worker
{

    /**
     * @var Monolog\Logger|null
     */
    protected $logger;

    protected $heartbeat_at;
    protected $heartbeat_liveness = 5;
    protected $heartbeat_interval = 2500;
    protected $reconnect          = 2500;
    protected $service;

    /**
     * @var ZMQSocket
     */
    protected $socket;

    /**
     * @var string
     */
    protected $dsn;

    public function setLogger(Logger $logger)
    {
        $this->logger = $logger;
        return $this;
    }
    
    /**
     * @see Monolog\Logger
     *
     * @param integer $level
     * @param string $message
     * @param array $context
     */
    public function addRecord($level, $message, array $context = array())
    {
        if (null !== $this->logger) {
            $this->logger->addRecord($level, $message, $context);
        }
    }

    /**
     * @param string $service
     * @return Worker
     */
    public function setService($service)
    {
        $this->service = $service;
        return $this;
    }

    /**
     * @param string $dsn
     * @return Worker
     */
    public function connect($dsn)
    {
        $this->dsn = $dsn;
        $context = new \ZMQContext();
        $this->socket = new \ZMQSocket($context, \ZMQ::SOCKET_DEALER);
        $this->socket->connect($this->dsn);
        $this->sendToBroker(Mdp::READY, $this->service);
        $this->liveness = $this->heartbeat_liveness;
        $this->heartbeat_at = microtime(true) + ($this->heartbeat_interval / 1000);
        return $this;
    }

    /**
     * Send a message to the broker
     *
     * @param integer $command
     * @param string $option
     * @param Message $message
     */
    public function sendToBroker($command, $option = null, Message $message = null)
    {
        if (null === $message) {
            $message = new Message();
        }

        if ($option) {
            $message->push($option);
        }

        $message->push($command);
        $message->push(Mdp::WORKER);
        $message->push("");
        $message->setSocket($this->socket)->send();

        if ($command != Mdp::HEARTBEAT) {
            $this->addRecord(Logger::DEBUG, 'Sending message to broker.', array(
                'command' => $command,
                'option'  => $option,
                'body'    => $message->getBody(),
            ));
        }
    }

    /**
     * @param Message $reply
     * @return Message
     */
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
                    $this->addRecord(Logger::DEBUG, 'Receiving Request', array(
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
                    $this->addRecord(Logger::ALERT, 'Unknow command', array(
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

}
