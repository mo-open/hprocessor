package org.mds.hprocessor.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Randall.mo
 */
public class DisruptorProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(DisruptorProcessorTest.class);

    private class EventValue {
        public EventValue(int value) {
            this.value = value;
        }

        int value;

        @Override
        public String toString() {
            return ""+value;
        }
    }

    @Test
    public void testDisruptorProcessor() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        DisruptorProcessor.Builder<EventValue> builder = DisruptorProcessor.newBuilder();
        DisruptorProcessor<EventValue> processor = builder
                .addNext(2, new ProcessorHandler<EventValue>() {
                    @Override
                    public void process(EventValue object) {
                        counter1.incrementAndGet();
                        log.info("1:------{}", object.value);
                        object.value = object.value * 2;
                    }
                }).addNext(2, new ProcessorHandler<EventValue>() {
                    @Override
                    public void process(EventValue object) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception ex) {

                        }
                        log.info("2:------{}", object.value);
                        counter2.incrementAndGet();
                        countDownLatch.countDown();
                    }
                }).build();

        for (int i = 0; i < 10; i++) {
            processor.submit(new EventValue(i));
        }
        countDownLatch.await(3, TimeUnit.SECONDS);
        Assert.assertEquals(counter1.get(), 10);
        Assert.assertEquals(counter2.get(), 10);
    }

    @Test
    public void testDisruptorProcessor2() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        DisruptorProcessor2.Builder<EventValue> builder = DisruptorProcessor2.newBuilder();
        DisruptorProcessor2<EventValue> processor = builder
                .addNext(2, new ProcessorHandler<EventValue>() {
                    @Override
                    public void process(EventValue object) {
                        counter1.incrementAndGet();
                        log.info("1:------{}", object.value);
                        object.value = object.value * 2;
                    }
                }).addNext(2, new ProcessorHandler<EventValue>() {
                    @Override
                    public void process(EventValue object) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception ex) {

                        }
                        log.info("2:------{}", object.value);
                        counter2.incrementAndGet();
                        countDownLatch.countDown();
                    }
                }).build();

        for (int i = 0; i < 10; i++) {
            processor.submit(new EventValue(i));
        }
        countDownLatch.await(3, TimeUnit.SECONDS);
        Assert.assertEquals(counter1.get(), 10);
        Assert.assertEquals(counter2.get(), 10);
    }

    @Test
    public void testTrySubmit() {
        DisruptorProcessor2.Builder<EventValue> builder = DisruptorProcessor2.newBuilder();
        DisruptorProcessor2<EventValue> processor = builder
                .setBufferSize(2)
                .addNext(1, new ProcessorHandler<EventValue>() {
                    @Override
                    public void process(EventValue object) {
                        try {
                            log.info("process-----"+object.value);
                            Thread.sleep(200);
                        } catch (Exception ex) {

                        }
                    }
                }).build();
        log.info("----buffer size:" + processor.bufferSize());
        processor.trySubmit(new EventValue(1));
        processor.trySubmit(new EventValue(2));
        processor.trySubmit(new EventValue(3));
        Assert.assertFalse(processor.trySubmit(new EventValue(4)));

        Assert.assertFalse(processor.trySubmit(new EventValue(5), 10, TimeUnit.MILLISECONDS));

        Assert.assertTrue(processor.trySubmit(new EventValue(3), 1000, TimeUnit.MILLISECONDS));

    }

}
