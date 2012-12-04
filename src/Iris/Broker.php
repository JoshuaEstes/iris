<?php

namespace Iris;

use Iris\Mdp;
use Iris\Message;
use Iris\WorkerQueue;

/**
 * @see http://rfc.zeromq.org/spec:7
 *
 * client requests routed to workers based on service names
 * detect disconnect using heartbeat
 * send work to workers based least recently used
 * recover from dead/disconnected workers by resending requests to other workers
 *
 * @todo Allow frontend for clients and backend for workers
 */
class Broker
{

    /**
     * @var \Monolog\Logger
     */
    protected $logger;

    protected $heartbeat_liveness = 3;
    protected $heartbeat_interval = 2500;
    protected $heartbeat_expiry;
    protected $heartbeat_at;

    /**
     * @var \ZMQSocket
     */
    protected $socket;
    protected $dsn;

    /**
     * Contains a list of services with all known workers.
     *
     * @var array
     */
    protected $services = array();

    /**
     * key value list of workers by address being the key
     *
     * @var array
     */
    protected $workers  = array();

    /**
     */
    public function __construct()
    {
        $context      = new \ZMQContext();
        $this->socket = new \ZMQSocket($context, \ZMQ::SOCKET_ROUTER);

        $this->heartbeat_expiry = $this->heartbeat_interval * $this->heartbeat_liveness;
        $this->heartbeat_at     = microtime(true) + ($this->heartbeat_interval / 1000);
    }

    /**
     * @param \Monolog\Logger $logger
     * @return Broker
     */
    public function setLogger(\Monolog\Logger $logger)
    {
        $this->logger = $logger;
        return $this;
    }

    /**
     * @param string $dsn
     * @return Broker
     */
    public function bind($dsn)
    {
        $this->socket->bind($dsn);
        return $this;
    }

    /**
     * Listens for incoming requests/replies from clients/workers
     */
    public function listen()
    {
        $read = $write = array();
        while (true) {
            $poll = new \ZMQPoll();
            $poll->add($this->socket, \ZMQ::POLL_IN);

            $events = $poll->poll($read, $write, $this->heartbeat_interval);

            if ($events) {
                $message = new Message($this->socket);
                $message->recv();

                $sender = $message->pop();
                $empty  = $message->pop();
                $header = $message->pop();

                switch($header) {
                case(Mdp::CLIENT):
                    $this->clientProcess($sender, $message);
                    break;
                case(Mdp::WORKER):
                    $this->workerProcess($sender, $message);
                    break;
                default:
                    $this->logger->alert('Unknown Header', array('header' => $header));
                    break;
                }
            }

            if (microtime(true) > $this->heartbeat_at) {
                $this->purgeWorkers();
                foreach ($this->workers as $worker) {
                    $this->workerSend($worker, Mdp::HEARTBEAT);
                }
                $this->heartbeat_at = microtime(true) + ($this->heartbeat_interval / 1000);
            }

            $this->logger->debug('Stats', array(
                'workers'  => count($this->workers),
                //'waiting'  => count($this->waiting),
                'servicesCount' => count($this->services),
                'service' => array_keys($this->services),
                'memory'   => array(
                    'memory_limit' => ini_get('memory_limit'),
                    'usage/peak'   => call_user_func(function () {
                        $convert = function($size) {
                            $unit = array('b','kb','mb','gb','tb','pb');
                            return round($size/pow(1024,($i=floor(log($size,1024)))),2).''.$unit[$i];
                        };
                        return $convert(memory_get_usage(true)).'/'.$convert(memory_get_peak_usage(true));
                    }),
                ),
            ));
        }
    }

    /**
     * Deletes workers that are expired
     */
    protected function purgeWorkers()
    {
        foreach ($this->workers as $worker) {
            if (empty($worker->expires_at) || microtime(true) > $worker->expires_at) {
                $this->deleteWorker($worker);
            }

        }
    }

    /**
     * Create a service or get a service
     *
     * @param string $name
     * @return stdClass
     */
    protected function getService($name)
    {
        if (!isset($this->services[$name])) {
            $service           = new \stdClass();
            $service->name     = $name;   // Name of service
            $service->workers  = array(); // List of workers tied to this service
            $service->requests = array(); // Requests from clients
            $service->waiting  = new WorkerQueue(); // Available Workers

            $this->services[$name] = $service;
            $this->logger->debug('New Service', array(
                'name' => $name,
            ));
        }

        return $this->services[$name];
    }

    /**
     * @param stdClass $service
     * @param \Iris\Message $message
     */
    protected function serviceDispatch($service, Message $message = null)
    {
        $this->logger->debug('Service Dispatching', array(
            'name'           => $service->name,
            'workers'        => count($service->workers),
            'waitingWorkers' => count($service->waiting),
            'totalWorkers'   => count($this->workers),
        ));

        if ($message) {
            $service->requests[] = $message;
        }

        $this->purgeWorkers();

        while (count($service->waiting) && count($service->requests)) {
            $worker  = $service->waiting->extract();
            $message = array_shift($service->requests);
            $this->workerSend($worker, Mdp::REQUEST, null, $message);
        }
    }

    /**
     * @param string $service_name
     * @param Message $message
     */
    public function serviceInternal($service_name, Message $message)
    {
        if ('mmi.service' === $service_name) {
            $name        = $message->getLast();
            $service     = $this->services[$name];
            $return_code = $service && $service->workers ? 200 : 404;
        } else {
            $return_code = 501;
            $this->logger->alert('501', array(
                'service_name' => $service_name,
            ));
        }

        $message->setLast($return_code);

        $client = $message->unwrap();

        $message->push($service_name);
        $message->push(Mdp::CLIENT);
        $message->wrap($client, "");
        $message->setSocket($this->socket)->send();

        $this->logger->debug('Sending message to client.', array(
            'client'      => $client,
            'service'     => $service_name,
            'return_code' => $return_code
        ));
    }

    /**
     * @param object $address
     * @return \stdClass
     */
    protected function getWorker($address)
    {
        $identity = Message::encode($address);
        if (!isset($this->workers[$identity])) {
            $worker               = new \stdClass();
            $worker->identity     = $identity;
            $worker->address      = $address;
            $worker->service_name = null;
            $worker->expires_at   = null;

            $this->workers[$identity] = $worker;
            $this->logger->debug('New Worker', array(
                'identity' => $worker->identity,
            ));
        }

        return $this->workers[$identity];
    }

    /**
     * @param \stdClass $worker
     * @param boolean $disconnect
     */
    protected function deleteWorker($worker, $disconnect = false)
    {
        $this->logger->debug('Deleting Worker', array(
            'worker'       => Message::encode($worker->address),
            'disconnect'   => $disconnect,
            'totalWorkers' => count($this->workers) - 1,
        ));

        if ($disconnect) {
            $this->workerSend($worker, Mdp::DISCONNECT);
        }

        unset($this->workers[$worker->identity]);

        $service = $this->getService($worker->service_name);
        if (isset($service->workers[$worker->identity])) {
            unset($this->services[$worker->service_name]->workers[$worker->identity]);
        }
    }

    protected function workerRemoveFromArray($worker, &$array)
    {
        $index = array_search($worker, $array);
        if (false !== $index) {
            unset($array[$index]);
        }
    }

    /**
     */
    protected function workerProcess($sender, $message)
    {
        $command      = $message->pop();
        $worker_ready = isset($this->workers[Message::encode($sender)]);
        $worker       = $this->getWorker($sender);

        switch($command) {
        case(Mdp::READY):
            if ($worker_ready) {
                $this->deleteWorker($worker, true);
            } elseif(strlen($sender) >= 4 && 'mmi.' == substr($sender, 0, 4)) {
                $this->deleteWorker($worker, true);
            } else {
                $service_name                        = $message->pop();
                $service                             = $this->getService($service_name);
                $service->workers[$worker->identity] = $worker;
                $worker->service_name                = $service_name;
                $this->workerWaiting($worker);
            }
            break;
        case(Mdp::REPLY):
            if ($worker_ready) {
                $client = $message->unwrap();
                $message->push($worker->service_name);
                $message->push(Mdp::CLIENT);
                $message->wrap($client, '');
                $message->setSocket($this->socket)->send();
                $this->workerWaiting($worker);
            } else {
                $this->deleteWorker($worker, true);
            }
            break;
        case(Mdp::HEARTBEAT):
            if ($worker_ready) {
                $worker->expires_at = microtime(true) + ($this->heartbeat_expiry / 1000);
            } else {
                $this->deleteWorker($worker, true);
            }
            break;
        case(Mdp::DISCONNECT):
            $this->deleteWorker($worker, true);
            break;
        default:
            $this->logger->alert('Unknown Command', array(
                'command' => $command,
            ));
            break;
        }
    }

    public function workerSend($worker, $command, $option = null, $message = null)
    {
        if (null === $message) {
            $message = new Message();
        }

        if (null !== $option) {
            $message->push($option);
        }

        if ($command != Mdp::HEARTBEAT) {
            $this->logger->debug('Sending worker message', array(
                'worker'  => Message::encode($worker->address),
                'option'  => $option,
                'command' => $command,
            ));
        }

        $message->push($command);
        $message->push(Mdp::WORKER);
        $message->wrap($worker->address, '');

        $message->setSocket($this->socket)->send();
    }

    public function workerWaiting($worker)
    {
        $service                      = $this->getService($worker->service_name);
        //$this->waiting[]            = $worker;
        //$worker->service->waiting[] = $worker;
        $worker->expires_at               = microtime(true) + ($this->heartbeat_expiry / 1000);
        $service->waiting->insert($worker);
        $this->logger->debug('workerWaiting', array(
            'worker' => Message::encode($worker->identity),
            'expiry' => $worker->expires_at,
        ));
        $this->serviceDispatch($service);
    }

    public function clientProcess($sender, $message)
    {
        $service_frame = $message->pop();
        $service = $this->getService($service_frame);
        $message->wrap($sender, '');

        if (substr($service_frame, 0, 4) == 'mmi.') {
            $this->serviceInternal($service_frame, $message);
        } else {
            $this->serviceDispatch($service, $message);
        }
    }

}
