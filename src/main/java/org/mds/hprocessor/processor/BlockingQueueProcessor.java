package org.mds.hprocessor.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Randall.mo
 */
public class BlockingQueueProcessor<T> implements Processor<T> {
    private static Logger log = LoggerFactory.getLogger(BlockingQueueProcessor.class);
    private static ExecutorService workerExecutor = Executors.newCachedThreadPool();
    private int bufferSize;
    private ProcessorWorker firstWorker;

    private BlockingQueueProcessor() {
    }

    @Override
    public int bufferSize() {
        return this.bufferSize;
    }

    @Override
    public void submit(T object) {
        this.firstWorker.submit(object);
    }

    @Override
    public boolean trySubmit(T object) {
        return this.firstWorker.trySubmit(object);
    }

    @Override
    public boolean trySubmit(T object, long timeout, TimeUnit timeUnit) {
        return this.firstWorker.trySubmit(object, timeout, timeUnit);
    }

    @Override
    public void close() throws IOException {
        if (this.workerExecutor != null) {
            this.workerExecutor.shutdownNow();
        }
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<T>();
    }

    public static class Builder<T> extends BatchBuilder<T, Builder<T>, BlockingQueueProcessor<T>> {
        private List<ProcessorWorker<T>> workers = new ArrayList();
        private boolean useBucket = false;


        public Builder<T> setUseBucket(boolean useBucket) {
            this.useBucket = useBucket;
            return this;
        }

        public Builder<T> addNext(int workerCount, ProcessorHandler<T> handler) {
            this.workers.add(new ProcessorWorker(workerCount, this.bufferSize, handler, this.useBucket));
            return this;
        }

        public Builder<T> addNext(ProcessorHandler<T>[] handlers) {
            this.workers.add(new ProcessorWorker(handlers, this.bufferSize, this.useBucket));
            return this;
        }

        public Builder<T> addNext(int workerCount, int batchSize, ProcessorBatchHandler<T> handler) {
            this.workers.add(new ProcessorWorker(workerCount, batchSize, this.bufferSize, handler, this.useBucket));
            return this;
        }

        public Builder<T> addNext(int batchSize, ProcessorBatchHandler<T>[] handlers) {
            this.workers.add(new ProcessorWorker(batchSize, this.bufferSize, handlers, this.useBucket));
            return this;
        }

        public BlockingQueueProcessor<T> build() {
            if (this.workers.isEmpty()) {
                throw new RuntimeException("Please add processor handler before build it.");
            }
            BlockingQueueProcessor<T> processor = new BlockingQueueProcessor();
            processor.firstWorker = this.workers.get(0);
            processor.bufferSize = this.bufferSize;
            ProcessorWorker nextWorker = null;
            for (int i = 0; i < this.workers.size(); i++) {
                if (i + 1 < this.workers.size()) {
                    nextWorker = this.workers.get(i + 1);
                } else {
                    nextWorker = null;
                }
                this.workers.get(i).start(nextWorker);
            }
            return processor;
        }
    }

    private static abstract class Handler<T> implements Runnable {
        ProcessorWorker nextWorker;
        LinkedBlockingQueue<T> blockingQueue;

        public void setNextWorker(ProcessorWorker nextWorker) {
            this.nextWorker = nextWorker;
        }

        public void submit(T value) {
            try {
                this.blockingQueue.put(value);
            } catch (Exception ex) {

            }
        }

        public boolean trySubmit(T value) {
            return this.blockingQueue.offer(value);
        }

        public boolean trySubmit(T value, long timeout, TimeUnit timeUnit) {
            try {
                return this.blockingQueue.offer(value, timeout, timeUnit);
            } catch (Exception ex) {
                return false;
            }
        }
    }

    private static class BatchEventHandler<T> extends Handler<T> {
        private ProcessorBatchHandler<T> processorBatchHandler;

        int batchSize = 10;

        public BatchEventHandler(ProcessorBatchHandler processorHandler,
                                 int batchSize, int bufferSize, LinkedBlockingQueue<T> blockingQueue) {
            this.processorBatchHandler = processorHandler;
            this.blockingQueue = blockingQueue;
            this.batchSize = batchSize;
            if (this.blockingQueue == null) {
                this.blockingQueue = new LinkedBlockingQueue(bufferSize);
            }
        }

        @Override
        public void run() {
            List<T> events = new ArrayList();
            T event = null;
            while (true) {
                try {
                    if (event == null)
                        event = blockingQueue.poll();
                    if (event != null) {
                        events.add(event);
                        if (events.size() == batchSize) {
                            this.processorBatchHandler.process(events);
                            if (this.nextWorker != null) {
                                for (T e : events) {
                                    this.nextWorker.submit(e);
                                }
                            }
                            events.clear();
                        }
                        event = null;
                    } else {
                        if (!events.isEmpty()) {
                            this.processorBatchHandler.process(events);
                            if (this.nextWorker != null) {
                                for (T e : events) {
                                    this.nextWorker.submit(e);
                                }
                            }
                            events.clear();
                        }
                        event = blockingQueue.take();
                    }
                } catch (InterruptedException ex) {
                    return;
                } catch (Exception ex) {
                    log.error("Failed to process event:" + ex);
                }
            }
        }
    }

    private static class EventHandler<T> extends Handler<T> {
        private ProcessorHandler<T> processorHandler;
        LinkedBlockingQueue<T> blockingQueue;

        public EventHandler(ProcessorHandler<T> processorHandler, int bufferSize,
                            LinkedBlockingQueue<T> blockingQueue) {
            this.processorHandler = processorHandler;
            this.blockingQueue = blockingQueue;
            if (this.blockingQueue == null) {
                this.blockingQueue = new LinkedBlockingQueue(bufferSize);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    T event = blockingQueue.take();
                    this.processorHandler.process(event);
                    if (this.nextWorker != null)
                        this.nextWorker.submit(event);
                } catch (InterruptedException ex) {
                    return;
                } catch (Exception ex) {
                    log.error("Failed to process event:" + ex);
                }
            }
        }
    }

    private static class ProcessorWorker<T> {
        protected LinkedBlockingQueue<T> blockingQueue;
        private Handler[] handlers;
        private static Random random = new Random();
        private boolean useBucket = false;

        public ProcessorWorker(int workerCount, int bufferSize,
                               ProcessorHandler processorHandler, boolean useBucket) {
            if (!useBucket || workerCount == 1) {
                this.blockingQueue = new LinkedBlockingQueue(bufferSize);
            }
            this.useBucket = useBucket;
            this.handlers = createHandlers(workerCount, bufferSize,
                    processorHandler, this.blockingQueue);
        }

        public ProcessorWorker(ProcessorHandler[] processorHandlers, int bufferSize, boolean useBucket) {
            if (!useBucket || processorHandlers.length == 1) {
                this.blockingQueue = new LinkedBlockingQueue(bufferSize);
            }
            this.useBucket = useBucket;
            this.handlers = new EventHandler[processorHandlers.length];
            for (int i = 0; i < processorHandlers.length; i++) {
                this.handlers[i] = new EventHandler(processorHandlers[i], bufferSize, this.blockingQueue);
            }
        }

        public ProcessorWorker(int workerCount, int batchSize, int bufferSize,
                               ProcessorBatchHandler processorHandler, boolean useBucket) {
            if (!useBucket || workerCount == 1) {
                this.blockingQueue = new LinkedBlockingQueue(bufferSize);
            }
            this.useBucket = useBucket;
            this.handlers = createHandlers(workerCount, batchSize, bufferSize, processorHandler,
                    this.blockingQueue);
        }

        public ProcessorWorker(int batchSize, int bufferSize, ProcessorBatchHandler[] processorHandlers,
                               boolean useBucket) {
            if (!useBucket || processorHandlers.length == 1) {
                this.blockingQueue = new LinkedBlockingQueue(bufferSize);
            }
            this.useBucket = useBucket;
            this.handlers = new BatchEventHandler[processorHandlers.length];
            for (int i = 0; i < processorHandlers.length; i++) {
                this.handlers[i] = new BatchEventHandler(processorHandlers[i],
                        batchSize, bufferSize, blockingQueue);
            }
        }

        public void start(ProcessorWorker nextWorker) {
            for (Handler handler : this.handlers) {
                handler.setNextWorker(nextWorker);
                workerExecutor.submit(handler);
            }
        }

        public void submit(T value) {
            try {
                if (this.blockingQueue != null) {
                    this.blockingQueue.put(value);
                } else {
                    this.handlers[random.nextInt(this.handlers.length)].submit(value);
                }
            } catch (Exception ex) {

            }
        }

        public boolean trySubmit(T value) {
            if (this.blockingQueue != null) {
                return this.blockingQueue.offer(value);
            } else {
                return this.handlers[random.nextInt(this.handlers.length)].trySubmit(value);
            }
        }

        public boolean trySubmit(T value, long timeout, TimeUnit timeUnit) {

            if (this.blockingQueue != null) {
                try {
                    return this.blockingQueue.offer(value, timeout, timeUnit);
                } catch (Exception ex) {
                    return false;
                }
            } else {
                return this.handlers[random.nextInt(this.handlers.length)].trySubmit(value, timeout, timeUnit);
            }
        }
    }

    private static Handler[] createHandlers(int workCount, int bufferSize, ProcessorHandler handler,
                                            LinkedBlockingQueue blockingQueue) {
        Handler[] processorHandlers = new EventHandler[workCount];
        EventHandler eventHandler = new EventHandler(handler, bufferSize, blockingQueue);
        for (int i = 0; i < workCount; i++) {
            processorHandlers[i] = eventHandler;
        }
        return processorHandlers;
    }

    private static Handler[] createHandlers(int workCount, int batchSize, int bufferSize, ProcessorBatchHandler handler,
                                            LinkedBlockingQueue blockingQueue) {
        Handler[] processorHandlers = new BatchEventHandler[workCount];
        BatchEventHandler eventHandler = new BatchEventHandler(handler, batchSize, bufferSize, blockingQueue);
        for (int i = 0; i < workCount; i++) {
            processorHandlers[i] = eventHandler;
        }
        return processorHandlers;
    }
}
