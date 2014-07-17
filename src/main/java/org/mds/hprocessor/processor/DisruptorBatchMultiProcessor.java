package org.mds.hprocessor.processor;

import com.google.common.base.Preconditions;
import com.lmax.disruptor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.lmax.disruptor.RingBuffer.createMultiProducer;
import static com.lmax.disruptor.RingBuffer.createSingleProducer;

/**
 * Created by Randall.mo on 14-5-30.
 */
public class DisruptorBatchMultiProcessor<T> extends AbstractDisruptorProcessor<T> {
    private static Logger log = LoggerFactory.getLogger(DisruptorBatchMultiProcessor.class);
    private static final int BATCH_BUFFER_SIZE = 1024 * 8;
    protected RingBuffer<HandlerEvent<List<T>>> batchRingBuffer;
    private ExecutorService workerExecutor;

    public DisruptorBatchMultiProcessor(int workerCount, int bufferSize, int batchSize, ProcessorBatchHandler<T> handler) {
        this(bufferSize, batchSize, createHandlers(workerCount, handler));
    }

    public DisruptorBatchMultiProcessor(int workerCount, int batchSize, ProcessorBatchHandler<T> handler) {
        this(BUFFER_SIZE, batchSize, createHandlers(workerCount, handler));
    }

    private static ProcessorBatchHandler[] createHandlers(int workCount, ProcessorBatchHandler handler) {
        Preconditions.checkArgument(handler != null, "handler argument can not be null");
        ProcessorBatchHandler[] processorHandlers = new ProcessorBatchHandler[workCount];
        for (int i = 0; i < workCount; i++) {
            processorHandlers[i] = handler;
        }
        return processorHandlers;
    }

    public DisruptorBatchMultiProcessor(int batchSize, ProcessorBatchHandler<T>[] processorHandlers) {
        this(BUFFER_SIZE, batchSize, processorHandlers);
    }

    public DisruptorBatchMultiProcessor(int bufferSize, int batchSize, ProcessorBatchHandler<T>[] processorHandlers) {
        super(bufferSize);
        Preconditions.checkArgument(processorHandlers != null && processorHandlers.length > 0,
                "handlers argument can not be empty");
        this.workerExecutor = Executors.newFixedThreadPool(processorHandlers.length + 1);
        BatchEventProcessor<HandlerEvent<T>> eventProcessor =
                new BatchEventProcessor(ringBuffer, ringBuffer.newBarrier(), new ProcessorEventHandler(batchSize));
        ringBuffer.addGatingSequences(eventProcessor.getSequence());
        this.workerExecutor.execute(eventProcessor);

        batchRingBuffer = createSingleProducer(new ProcessorEventFactory<List<T>>(),
                BATCH_BUFFER_SIZE, new BlockingWaitStrategy());

        BatchEventHandler[] batchEventHandlers = new BatchEventHandler[processorHandlers.length];

        for (int i = 0; i < processorHandlers.length; i++) {
            batchEventHandlers[i] = new BatchEventHandler(processorHandlers[i]);
        }

        WorkerPool<HandlerEvent<List<T>>> workerPool =
                new WorkerPool(batchRingBuffer,
                        batchRingBuffer.newBarrier(),
                        new FatalExceptionHandler(),
                        batchEventHandlers);
        batchRingBuffer.addGatingSequences(workerPool.getWorkerSequences());

        workerPool.start(this.workerExecutor);
    }

    @Override
    public void close() throws IOException {
        if (this.workerExecutor != null) {
            this.workerExecutor.shutdown();
        }
    }

    private void submitBatch(List<T> events) {
        long sequence = this.batchRingBuffer.next();
        HandlerEvent event = this.batchRingBuffer.get(sequence);
        event.event = events;
        this.batchRingBuffer.publish(sequence);
    }

    private class BatchEventHandler<T> implements WorkHandler<HandlerEvent<List<T>>> {
        ProcessorBatchHandler processorBatchHandler;

        public BatchEventHandler(ProcessorBatchHandler<T> processorBatchHandler) {
            this.processorBatchHandler = processorBatchHandler;
        }

        @Override
        public void onEvent(HandlerEvent<List<T>> event) throws Exception {
            this.processorBatchHandler.process(event.event);
        }
    }

    private class ProcessorEventHandler implements EventHandler<HandlerEvent<T>> {
        List<T> events = new ArrayList();
        int batchSize = 20;

        public ProcessorEventHandler(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void onEvent(HandlerEvent<T> event, long sequence, boolean endOfBatch) throws Exception {
            this.events.add(event.event);
            if (this.events.size() == this.batchSize || endOfBatch) {
                submitBatch(this.events);
                this.events = new ArrayList<>();
            }
        }
    }
}
