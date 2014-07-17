package org.mds.hprocessor.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Randall.mo
 */
public class BlockingQueueProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(BlockingQueueProcessorTest.class);

    @Test
    public void testSubmit() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        BlockingQueueProcessor<Integer> processor = BlockingQueueProcessor.<Integer>newBuilder()
                .addNext(2, 10, new ProcessorBatchHandler() {
                    @Override
                    public void process(List objects) {

                    }
                }).addNext(2, new ProcessorHandler<Integer>() {
                    @Override
                    public void process(Integer object) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception ex) {

                        }
                        counter.incrementAndGet();
                        countDownLatch.countDown();
                    }
                }).build();

        for (int i = 0; i < 10; i++) {
            processor.submit(i);
        }
        countDownLatch.await(3, TimeUnit.SECONDS);
        Assert.assertEquals(counter.get(), 10);
    }

    @Test
    public void testTrySubmit() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        BlockingQueueProcessor.Builder<Integer> builder = BlockingQueueProcessor.newBuilder();
        BlockingQueueProcessor<Integer> processor = builder
                .setBufferSize(2)
                .addNext(1, new ProcessorHandler<Integer>() {
                    @Override
                    public void process(Integer object) {
                        try {
                            Thread.sleep(200);
                        } catch (Exception ex) {

                        }
                        countDownLatch.countDown();
                    }
                }).build();
        processor.submit(1);
        processor.submit(2);
        processor.submit(3);
        Assert.assertFalse(processor.trySubmit(4));
        Assert.assertFalse(processor.trySubmit(5, 10, TimeUnit.MILLISECONDS));
        Assert.assertTrue(processor.trySubmit(5, 1000, TimeUnit.MILLISECONDS));
    }
}
