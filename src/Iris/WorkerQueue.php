<?php

namespace Iris;

/**
 * This is used by the broker to keep a list of workers that is
 * sorted by least recently used.
 *
 * $worker = new \stdClass();
 * $worker->expires_at = last heartbeat + timeout
 */
class WorkerQueue extends \SplHeap
{

    /**
     * Positive if $value1 < $value2
     * Negative if $value1 > $value2
     *
     * @return integer
     */
    public function compare($value1, $value2)
    {
        return $value1->expires_at > $value2->expires_at ? -1 : 1;
    }

}
