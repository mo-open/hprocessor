package org.mds.hprocessor.processor;

import com.lmax.disruptor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.lmax.disruptor.RingBuffer.createMultiProducer;

/**
 * This processor has more than 3 times throughput than BlockingQueue
 * Created by Randall.mo on 14-4-21.
 */
public class DisruptorProcessor<T> extends AbstractDisruptorProcessor<T> {
    private static Logger log = LoggerFactory.getLogger(DisruptorProcessor.class);
    protected static ExecutorService workerExecutor;

    private DisruptorProcessor(int bufferSize) {
        super(bufferSize);
        this.workerExecutor = Executors.newCachedThreadPool();
    }

    private static class DisruptorHandler<T0> implements WorkHandler<HandlerEvent<T0>> {
        ProcessorHandler<T0> processorHandler;

        public DisruptorHandler(ProcessorHandler<T0> processorHandler) {
            this.processorHandler = processorHandler;
        }

        @Override
        public void onEvent(HandlerEvent<T0> event) throws Exception {
            try {
                if (this.processorHandler != null) {
                    this.processorHandler.process(event.event);
                }
            } catch (Exception ex) {
                log.error("Failed to handle value '{}'", event.event, ex);
            }
        }
    }

    public void close() {
        if (this.workerExecutor != null)
            this.workerExecutor.shutdown();
    }

    public static <T> Builder<T> newBuilder() {
        return new <T>Builder<T>();
    }

    public static class Builder<T> extends SingleBuilder<T, Builder<T>, DisruptorProcessor<T>> {

        private List<ProcessorWorker> workers = new ArrayList();

        private Builder() {
        }

        public Builder<T> addNext(int workerCount, ProcessorHandler<T> handler) {
            this.workers.add(new ProcessorWorker(workerCount, handler));
            return this;
        }

        public Builder<T> addNext(ProcessorHandler<T>[] handlers) {
            this.workers.add(new ProcessorWorker(handlers));
            return this;
        }

        public DisruptorProcessor<T> build() {
            if (this.workers.isEmpty()) {
                throw new RuntimeException("Please add processor handler before build it.");
            }
            DisruptorProcessor<T> disruptorProcessor = new DisruptorProcessor(this.bufferSize);

            SequenceBarrier barrier = disruptorProcessor.ringBuffer.newBarrier();

            for (int i = 0; i < this.workers.size(); i++) {
                workers.get(i).build(disruptorProcessor, barrier);
                barrier = disruptorProcessor.ringBuffer.newBarrier(workers.get(i).sequences);
            }
            disruptorProcessor.ringBuffer.addGatingSequences
                    (this.workers.get(this.workers.size() - 1).sequences);
            for (ProcessorWorker worker : this.workers) {
                worker.start(workerExecutor);
            }
            return disruptorProcessor;
        }
    }


    private static class ProcessorWorker<T> {
        private Sequence workSequence = new Sequence(-1);
        private ProcessorHandler<T>[] processorHandlers;
        private WorkProcessor[] workProcessors;
        private Sequence[] sequences;

        public ProcessorWorker(int workerCount, ProcessorHandler<T> handler) {
            this(createHandlers(workerCount, handler));
        }

        public ProcessorWorker(ProcessorHandler<T>[] processorHandlers) {
            this.processorHandlers = processorHandlers;
            this.sequences = new Sequence[processorHandlers.length];
            this.workProcessors = new WorkProcessor[processorHandlers.length];
        }

        public void build(DisruptorProcessor<T> processor, SequenceBarrier barrier) {
            DisruptorHandler<T>[] handlers = new DisruptorHandler[this.processorHandlers.length];
            for (int i = 0; i < this.processorHandlers.length; i++) {
                handlers[i] = new DisruptorHandler(this.processorHandlers[i]);
                WorkProcessor workProcessor = new WorkProcessor<HandlerEvent<T>>(processor.ringBuffer,
                        barrier, handlers[i], new IgnoreExceptionHandler(),
                        workSequence);
                this.workProcessors[i] = workProcessor;
                sequences[i] = workProcessor.getSequence();
            }
        }

        public void start(ExecutorService executor) {
            for (WorkProcessor workProcessor : this.workProcessors) {
                executor.submit(workProcessor);
            }
        }

        public Sequence workSequence() {
            return this.workSequence;
        }

        public Sequence[] sequences() {
            return this.sequences;
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
