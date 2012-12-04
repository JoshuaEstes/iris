<?php

namespace Iris;

final class Mdp
{
    const CLIENT     = 'MDPC01';
    const WORKER     = 'MDPW01';
    const READY      = 0x01;
    const REQUEST    = 0x02;
    const REPLY      = 0x03;
    const HEARTBEAT  = 0x04;
    const DISCONNECT = 0x05;
}
