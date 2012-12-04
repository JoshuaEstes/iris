<?php


class WorkerQueueTest extends \PHPUnit_Framework_TestCase
{

    public function testCompare()
    {
        $queue = new \Iris\WorkerQueue();

        $worker1             = new \stdClass();
        $worker1->address    = '@zero';
        $worker1->expires_at = 0;
        $worker2             = new \stdClass();
        $worker2->address    = '@five';
        $worker2->expires_at = 5;
        $worker3             = new \stdClass();
        $worker3->address    = '@ten';
        $worker3->expires_at = 10;

        $queue->insert($worker2);
        $queue->insert($worker3);
        $queue->insert($worker1);

        // The queue should be ordered from least to greatest

        $queue->rewind();

        $current = $queue->extract();
        $this->assertEquals('@zero', $current->address);
        $current = $queue->extract();
        $this->assertEquals('@five', $current->address);
        $current = $queue->extract();
        $this->assertEquals('@ten', $current->address);
    }

}
