<?php

namespace Iris;

class Message
{
    protected $frames = array();
    protected $socket;

    public function __construct($socket = null)
    {
        $this->setSocket($socket);
    }

    public static function encode($data)
    {
        return '@' . bin2hex($data);
    }

    public static function decode($data)
    {
        return pack('H*', substr($data, 1));
    }

    public function setSocket($socket)
    {
        $this->socket = $socket;
        return $this;
    }

    public function recv()
    {
        if (null === $this->socket) {
            throw new \Exception('Socket not set');
        }

        $this->frames = array();
        while (true) {
            $this->frames[] = $this->socket->recv();
            if (!$this->socket->getSockOpt(\ZMQ::SOCKOPT_RCVMORE)) {
                break;
            }
        }

        return $this;
    }

    public function send()
    {
        if (null === $this->socket) {
            throw new \Exception('Socket not set.');
        }

        $count = count($this->frames);
        $i     = 1;

        foreach ($this->frames as $frame) {
            $mode = $i++ == $count ? null : \ZMQ::MODE_SNDMORE;
            $this->socket->send($frame, $mode);
        }

        return $this;
    }

    public function frames()
    {
        return count($this->frames);
    }

    public function push($frame)
    {
        array_unshift($this->frames, $frame);
    }

    public function pop()
    {
        return array_shift($this->frames);
    }

    public function getAddress()
    {
        $address = count($this->frames) ? $this->frames[0] : null;
        return (strlen($address) == 17 && $address[0] == 0) ? self::encode($address) : $address;
    }

    public function wrap($address, $delim = null)
    {
        if (null !== $delim) {
            $this->push($delim);
        }

        if ($address[0] == '@' && strlen($address) == 33) {
            $address = self::decode($address);
        }

        $this->push($address);

        return $this;
    }

    public function unwrap()
    {
        $address = $this->pop();
        if (!$this->getAddress()) {
            $this->pop();
        }

        return $address;
    }

    public function getLast()
    {
        return $this->frames[count($this->frames) - 1];
    }

    public function setLast($frame)
    {
        $this->frames[count($this->frames) - 1] = $frame;
        return $this;
    }

    public function setBody($body)
    {
        $pos = count($this->frames);
        if ($pos > 0) {
            $pos = $pos - 1;
        }
        $this->frames[$pos] = $body;
        return $this;
    }

    public function getBody()
    {
        return $this->getLast();
    }

}
