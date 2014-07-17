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
 * Created by Randall.mo on 14-6-9.
 */
public class DisruptorBatchMultiProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(DisruptorBatchMultiProcessorTest.class);

    @Test
    public void testSubmit() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        Processor<Integer> processor = new DisruptorBatchMultiProcessor<>(2, 10, new ProcessorBatchHandler<Integer>() {
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
        });

        for (int i = 0; i < 10; i++) {
            processor.submit(i);
        }
        countDownLatch.await(3, TimeUnit.SECONDS);
        Assert.assertEquals(counter.get(), 10);
    }
}
