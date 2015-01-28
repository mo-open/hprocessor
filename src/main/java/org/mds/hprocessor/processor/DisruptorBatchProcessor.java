package org.mds.hprocessor.processor;

import com.lmax.disruptor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Randall.mo on 14-6-9.
 */
public class DisruptorBatchProcessor<T> extends AbstractDisruptorProcessor<T> {
    private static Logger log = LoggerFactory.getLogger(DisruptorBatchProcessor.class);
    private static ExecutorService executorService = Executors.newCachedThreadPool();
    private static Random random = new Random();

    private DisruptorBatchProcessor() {

    }

    private DisruptorBatchProcessor(int bufferSize) {
        super(bufferSize);
    }

    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private static class ProcessorEventHandler<T0> implements EventHandler<HandlerEvent<T0>> {
        private ExecutorService handlerExecutor;
        ProcessorHandler<T0>[] processorHandlers;
        ProcessorHandler<T0> singleHandler;
        List<Future> tasks = new CopyOnWriteArrayList<>();

        public ProcessorEventHandler(ProcessorHandler<T0>[] processorHandlers) {
            this.processorHandlers = processorHandlers;
            this.handlerExecutor = Executors.newFixedThreadPool(processorHandlers.length);
        }

        public ProcessorEventHandler(ProcessorHandler<T0> processorHandler) {
            this.singleHandler = processorHandler;
        }

        @Override
        public void onEvent(final HandlerEvent<T0> event, long sequence, boolean endOfBatch) throws Exception {
            if (this.singleHandler != null) {
                this.singleHandler.process(event.event);
                return;
            }
            tasks.add(this.handlerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    processorHandlers[random.nextInt(processorHandlers.length)].process(event.event);
                }
            }));
            if (endOfBatch) {
                for (Future task : this.tasks) {
                    try {
                        task.get();
                    } catch (Exception ex) {

                    }
                }
            }
            this.tasks.clear();
        }
    }

    private static class ProcessorBatchEventHandler<T0> implements EventHandler<HandlerEvent<T0>> {
        private ExecutorService handlerExecutor = Executors.newCachedThreadPool();
        ProcessorBatchHandler<T0>[] processorHandlers;
        List<T0> events = new ArrayList();
        List<Future> tasks = new CopyOnWriteArrayList<>();
        int batchSize = 20;

        public ProcessorBatchEventHandler(int batchSize, ProcessorBatchHandler<T0>[] processorHandlers) {
            this.batchSize = batchSize;
            this.processorHandlers = processorHandlers;
        }

        @Override
        public void onEvent(HandlerEvent<T0> event, long sequence, boolean endOfBatch) throws Exception {
            this.events.add(event.event);
            if (this.events.size() == this.batchSize || endOfBatch) {
                this.tasks.add(this.handlerExecutor.submit(new EventProcessTask(this.events)));
                this.events = new ArrayList<>();
            }
            if (endOfBatch) {
                for (Future task : this.tasks) {
                    try {
                        task.get();
                    } catch (Exception ex) {

                    }
                }
            }
            this.tasks.clear();
        }

        private class EventProcessTask implements Runnable {
            List<T0> events;

            public EventProcessTask(List<T0> events) {
                this.events = events;
            }

            @Override
            public void run() {
                processorHandlers[random.nextInt(processorHandlers.length)].
                        process(this.events);
            }
        }
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<T>();
    }

    public static class Builder<T> extends BatchBuilder<T, Builder<T>, DisruptorBatchProcessor<T>> {
        private List<EventHandler<T>> handlers = new ArrayList();
        private EventHandler<T> singleHandler;

        private Builder() {
        }

        public Builder<T> setSingleHandler(ProcessorHandler<T> handler) {
            this.singleHandler = new ProcessorEventHandler(handler);
            return this;
        }

        public Builder<T> addNext(int workerCount, ProcessorHandler<T> handler) {
            this.handlers.add(new ProcessorEventHandler(createHandlers(workerCount, handler)));
            return this;
        }

        public Builder<T> addNext(ProcessorHandler<T>[] handlers) {
            this.handlers.add(new ProcessorEventHandler(handlers));
            return this;
        }

        public Builder<T> addNext(int workerCount, int batchSize, ProcessorBatchHandler<T> batchHandler) {
            this.handlers.add(new ProcessorBatchEventHandler(batchSize, createHandlers(workerCount, batchHandler)));
            return this;
        }

        public Builder<T> addNext(int batchSize, ProcessorBatchHandler<T>[] batchHandlers) {
            this.handlers.add(new ProcessorBatchEventHandler(batchSize, batchHandlers));
            return this;
        }

        public DisruptorBatchProcessor<T> build() {
            if (this.handlers.isEmpty()) {
                throw new RuntimeException("Please add processor handler before build it.");
            }
            DisruptorBatchProcessor<T> disruptorProcessor = new DisruptorBatchProcessor(this.bufferSize);

            SequenceBarrier barrier = disruptorProcessor.ringBuffer.newBarrier();
            if (this.singleHandler != null) {
                this.handlers.clear();
                this.handlers.add(this.singleHandler);
            }
            BatchEventProcessor<HandlerEvent<T>> eventProcessor = null;
            for (EventHandler handler : this.handlers) {
                eventProcessor = new BatchEventProcessor(disruptorProcessor.ringBuffer,
                        barrier, handler);
                executorService.submit(eventProcessor);
                barrier = disruptorProcessor.ringBuffer.newBarrier(eventProcessor.getSequence());
            }
            disruptorProcessor.ringBuffer.addGatingSequences(eventProcessor.getSequence());
            return disruptorProcessor;
        }
    }

    private static ProcessorHandler[] createHandlers(int workCount, ProcessorHandler handler) {
        ProcessorHandler[] processorHandlers = new ProcessorHandler[workCount];
        for (int i = 0; i < workCount; i++) {
            processorHandlers[i] = handler;
        }
        return processorHandlers;
    }

    private static ProcessorBatchHandler[] createHandlers(int workCount, ProcessorBatchHandler handler) {
        ProcessorBatchHandler[] processorHandlers = new ProcessorBatchHandler[workCount];
        for (int i = 0; i < workCount; i++) {
            processorHandlers[i] = handler;
        }
        return processorHandlers;
    }
}
