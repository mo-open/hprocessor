package org.mds.hprocessor.processor;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.lmax.disruptor.RingBuffer.createMultiProducer;

/**
 * Created by Randall.mo on 14-5-30.
 */
public abstract class AbstractDisruptorProcessor<T> implements Processor<T> {
    private static Logger log = LoggerFactory.getLogger(AbstractDisruptorProcessor.class);
    protected static final int BUFFER_SIZE = 1024 * 8;
    protected RingBuffer<HandlerEvent<T>> ringBuffer;
    protected int bufferSize = BUFFER_SIZE;

    protected AbstractDisruptorProcessor() {
        this(BUFFER_SIZE);
    }

    protected AbstractDisruptorProcessor(int bufferSize) {
        this.bufferSize = calculateBufferSize(bufferSize);
        ringBuffer = createMultiProducer(new ProcessorEventFactory<T>(), this.bufferSize,
                new BlockingWaitStrategy());
    }

    private static int calculateBufferSize(int bufferSize) {
        return (int) Math.round(Math.pow(2, Math.ceil(Math.log(bufferSize) / Math.log(2))));
    }

    @Override
    public int bufferSize() {
        return this.bufferSize;
    }

    @Override
    public void submit(T object) {
        while (!ringBuffer.hasAvailableCapacity(1)) {
            LockSupport.parkNanos(100);
        }
        long sequence = ringBuffer.next();
        HandlerEvent event = ringBuffer.get(sequence);
        event.event = object;
        ringBuffer.publish(sequence);
    }

    @Override
    public boolean trySubmit(T object) {
        try {
            long sequence = ringBuffer.tryNext();
            HandlerEvent event = ringBuffer.get(sequence);
            event.event = object;
            ringBuffer.publish(sequence);
            return true;
        } catch (InsufficientCapacityException ex) {
            log.warn("Processor no space");
        }

        return false;
    }

    @Override
    public boolean trySubmit(T object, long timeout, TimeUnit timeUnit) {
        long retryTimes = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        do {
            try {
                long sequence = ringBuffer.tryNext();
                HandlerEvent event = ringBuffer.get(sequence);
                event.event = object;
                ringBuffer.publish(sequence);
                return true;
            } catch (InsufficientCapacityException ex) {
                try{
                    Thread.sleep(1);
                }catch (Exception ex1){

                }
            }
        } while (retryTimes-- > 0);
        return false;
    }

    public static class HandlerEvent<T1> {
        T1 event;
    }

    protected static class ProcessorEventFactory<T2> implements EventFactory {

        @Override
        public HandlerEvent<T2> newInstance() {
            return new HandlerEvent<T2>();
        }
    }
}
