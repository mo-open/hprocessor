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
public class DisruptorBatchProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(DisruptorProcessorTest.class);

    @Test
    public void testSubmit() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        DisruptorBatchProcessor<Integer> processor = DisruptorBatchProcessor.<Integer>newBuilder()
                .setBufferSize(10).addNext(2, 2, new ProcessorBatchHandler<Integer>() {
                    @Override
                    public void process(List<Integer> objects) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception ex) {

                        }
                        log.info("process --{}", counter.addAndGet(objects.size()));
                        for (int i = 0; i < objects.size(); i++)
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
        DisruptorBatchProcessor.Builder<Integer> builder = DisruptorBatchProcessor.<Integer>newBuilder();
        DisruptorBatchProcessor<Integer> processor = builder
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
        processor.trySubmit(1);
        processor.trySubmit(2);
        processor.trySubmit(3);
        Assert.assertFalse(processor.trySubmit(4));
        Assert.assertFalse(processor.trySubmit(5, 10, TimeUnit.MILLISECONDS));
        Assert.assertTrue(processor.trySubmit(6, 1000, TimeUnit.MILLISECONDS));
    }
}
