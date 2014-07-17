High Performance Processor
=========
including:
   1. Processor Framework (using Disruptor or BlockingQueue)
   2. Memecache Processor based on Processor Framework
      Generally, the get operation has no a high throughput and getBulk has better performance.
      This processor gathers the scattered get operations to do the getBulk to improve the get performance.
   3.  Redis Processor based on Processor Framework
      Generally,the pipeline operation is better.
      This processor gathers the scattered operations to do the pipeline operation to improve the performance.

How to use?
   1. Processor Framework

   2. Memcache Processor

   3. Redis Processor