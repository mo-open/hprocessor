package org.mds.hprocessor.memcache;

import org.mds.hprocessor.processor.BlockingQueueProcessor;
import org.mds.hprocessor.processor.DisruptorBatchProcessor;
import org.mds.hprocessor.processor.DisruptorProcessor;
import org.mds.hprocessor.processor.Processor;

import java.util.concurrent.TimeUnit;

/**
 * Created by Randall.mo on 14-7-14.
 */
public abstract class MemcacheProcessor {
    protected final static int DEFAULT_BUFSIZE = 1024 * 16;
    protected final static int DEFAULT_CALLBACK_BUFSIZE = 1024 * 8;
    protected final static int DEFAULT_SUBMIT_TIMEOUT = 1000;
    protected final static int DEFAULT_GET_TIMEOUT = 3000;
    protected final static TimeUnit DEFAULT_TIMEUNIT = TimeUnit.MILLISECONDS;

    protected int submitTimeout = DEFAULT_SUBMIT_TIMEOUT;
    protected TimeUnit submitTimeUnit = DEFAULT_TIMEUNIT;
    protected int getTimeout = DEFAULT_GET_TIMEOUT;
    protected TimeUnit getTimeUnit = DEFAULT_TIMEUNIT;

    public enum ProcessorType {
        QUEUE, DISRUPTOR;
    }

    public static abstract class ProcessorBuilder<T0 extends Object, T1 extends Object,
            T extends ProcessorBuilder<T0, T1, T, ? extends MemcacheProcessor>,
            K extends MemcacheProcessor> {
        ProcessorType processorType = ProcessorType.DISRUPTOR;
        int bufferSize = DEFAULT_BUFSIZE;
        int callbackBufferSize = DEFAULT_CALLBACK_BUFSIZE;
        int callbackWorkers = 4;
        int submitTimeout = DEFAULT_SUBMIT_TIMEOUT;
        TimeUnit submitTimeUnit = DEFAULT_TIMEUNIT;
        int getTimeout = DEFAULT_GET_TIMEOUT;
        TimeUnit getTimeUnit = DEFAULT_TIMEUNIT;

        public T setSubmitTimeout(int timeout, TimeUnit timeUnit) {
            this.submitTimeout = timeout;
            this.submitTimeUnit = timeUnit;
            return (T) this;
        }

        public T setGetTimeout(int timeout, TimeUnit timeUnit) {
            this.getTimeout = timeout;
            this.getTimeUnit = timeUnit;
            return (T) this;
        }

        public T setBufferSize(int bufferSize) {
            if (bufferSize > 0)
                this.bufferSize = bufferSize;
            return (T) this;
        }

        public T setCallbackBufferSize(int callbackBufferSize) {
            if (callbackBufferSize > 0)
                this.callbackBufferSize = callbackBufferSize;
            return (T) this;
        }

        public T setCallbackWorkers(int callbackWorkers) {
            if (callbackWorkers > 0) {
                this.callbackWorkers = callbackWorkers;
            }
            return (T) this;
        }

        public T setProcessorType(ProcessorType processorType) {
            this.processorType = processorType;
            return (T) this;
        }

        protected void setTimeout(MemcacheProcessor memcacheProcessor) {
            memcacheProcessor.getTimeout = this.getTimeout;
            memcacheProcessor.getTimeUnit = this.getTimeUnit;
            memcacheProcessor.submitTimeout = this.submitTimeout;
            memcacheProcessor.submitTimeUnit = this.submitTimeUnit;
        }

        protected Processor.BatchBuilder builder() {
            if (this.processorType == ProcessorType.QUEUE) {
                return BlockingQueueProcessor.<T0>newBuilder()
                        .setBufferSize(this.bufferSize);
            } else if (this.processorType == ProcessorType.DISRUPTOR)
                return DisruptorBatchProcessor.<T0>newBuilder()
                        .setBufferSize(this.bufferSize);
            return null;
        }

        protected Processor.SingleBuilder callbackBuilder() {
            if (this.processorType == ProcessorType.QUEUE) {
                return BlockingQueueProcessor.<T1>newBuilder()
                        .setBufferSize(this.callbackBufferSize);
            } else if (this.processorType == ProcessorType.DISRUPTOR)
                return DisruptorProcessor.<T1>newBuilder()
                        .setBufferSize(this.callbackBufferSize);
            return null;
        }

        public abstract K build();

    }
}
