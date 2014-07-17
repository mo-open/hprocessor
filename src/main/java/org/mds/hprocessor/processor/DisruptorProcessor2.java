package org.mds.hprocessor.processor;

import com.lmax.disruptor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Randall.mo
 */
public class DisruptorProcessor2<T> implements Processor<T> {
    private static Logger log = LoggerFactory.getLogger(DisruptorProcessor.class);
    protected static ExecutorService workerExecutor;
    private Processor<T> firstWorker;
    private int bufferSize;

    private DisruptorProcessor2() {
        this.workerExecutor = Executors.newCachedThreadPool();
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
        if (this.workerExecutor != null)
            this.workerExecutor.shutdown();
    }

    public static <T>Builder<T> newBuilder() {
        return new Builder<T>();
    }

    public static class Builder<T>
            extends SingleBuilder<T, Builder<T>, DisruptorProcessor2<T>> {
        private List<ProcessorWorker> workers = new ArrayList();

        private Builder() {
        }

        public Builder<T> addNext(int workerCount, ProcessorHandler<T> handler) {
            this.workers.add(new ProcessorWorker(workerCount, this.bufferSize, handler));
            return this;
        }

        public Builder<T> addNext(ProcessorHandler<T>[] handlers) {
            this.workers.add(new ProcessorWorker(this.bufferSize, handlers));
            return this;
        }

        public DisruptorProcessor2<T> build() {
            if (this.workers.isEmpty()) {
                throw new RuntimeException("Please add processor handler before build it.");
            }

            DisruptorProcessor2<T> disruptorProcessor = new DisruptorProcessor2();
            disruptorProcessor.bufferSize = this.bufferSize;
            ProcessorWorker nextWorker = null;
            for (int i = 0; i < this.workers.size(); i++) {
                if (i + 1 < this.workers.size()) {
                    nextWorker = this.workers.get(i + 1);
                } else {
                    nextWorker = null;
                }
                this.workers.get(i).start(workerExecutor, nextWorker);
            }
            disruptorProcessor.firstWorker = this.workers.get(0);
            return disruptorProcessor;
        }

    }

    private static class DisruptorHandler<T0> implements WorkHandler<AbstractDisruptorProcessor.HandlerEvent<T0>> {
        ProcessorHandler<T0> processorHandler;
        ProcessorWorker nextWorker;

        public DisruptorHandler(ProcessorHandler<T0> processorHandler) {
            this.processorHandler = processorHandler;
        }

        public void setNextWorker(ProcessorWorker nextWorker) {
            this.nextWorker = nextWorker;
        }

        @Override
        public void onEvent(AbstractDisruptorProcessor.HandlerEvent<T0> event) throws Exception {
            try {
                if (this.processorHandler != null) {
                    this.processorHandler.process(event.event);
                    if (this.nextWorker != null)
                        this.nextWorker.submit(event.event);
                }
            } catch (Exception ex) {
                log.error("Failed to handle value '{}'", event.event, ex);
            }
        }
    }

    private static class ProcessorWorker<T> extends AbstractDisruptorProcessor {
        private Sequence workSequence = new Sequence(-1);
        private ProcessorHandler<T>[] processorHandlers;

        ProcessorWorker(int workerCount, int bufferSize, ProcessorHandler processorHandler) {
            this(bufferSize, createHandlers(workerCount, processorHandler));
        }

        ProcessorWorker(int bufferSize, ProcessorHandler[] processorHandlers) {
            super(bufferSize);
            this.processorHandlers = processorHandlers;
        }

        public void start(ExecutorService executor, ProcessorWorker nextWorker) {
            DisruptorHandler<T>[] handlers = new DisruptorHandler[this.processorHandlers.length];
            WorkProcessor[] workProcessors = new WorkProcessor[this.processorHandlers.length];
            Sequence[] sequences = new Sequence[this.processorHandlers.length];
            SequenceBarrier barrier = this.ringBuffer.newBarrier();
            for (int i = 0; i < this.processorHandlers.length; i++) {
                handlers[i] = new DisruptorHandler(this.processorHandlers[i]);
                handlers[i].setNextWorker(nextWorker);
                WorkProcessor workProcessor = new WorkProcessor<HandlerEvent<T>>(this.ringBuffer,
                        barrier, handlers[i], new IgnoreExceptionHandler(),
                        workSequence);
                workProcessors[i] = workProcessor;
                sequences[i] = workProcessor.getSequence();
            }
            this.ringBuffer.addGatingSequences(sequences);

            for (WorkProcessor workProcessor : workProcessors) {
                executor.submit(workProcessor);
            }
        }


        @Override
        public void close() throws IOException {

        }
    }

    private static ProcessorHandler[] createHandlers(int workCount, ProcessorHandler handler) {
        ProcessorHandler[] processorHandlers = new ProcessorHandler[workCount];
        for (int i = 0; i < workCount; i++) {
            processorHandlers[i] = handler;
        }
        return processorHandlers;
    }
}
