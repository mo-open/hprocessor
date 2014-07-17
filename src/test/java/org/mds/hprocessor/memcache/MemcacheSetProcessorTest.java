package org.mds.hprocessor.memcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Randall.mo on 14-6-9.
 */
public class MemcacheSetProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(MemcacheSetProcessorTest.class);
    private static Map<String, Object> cache = new HashMap();

    private static class TestMemcacheSetter implements MemcacheSetter {
        CountDownLatch countDownLatch;

        public TestMemcacheSetter(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void set(String key, int exp, Object value) {
            cache.put(key, value);
            if (this.countDownLatch != null)
                countDownLatch.countDown();
        }

        @Override
        public void syncSet(String key, int exp, Object value) {
            cache.put(key, value);
            if (this.countDownLatch != null)
                countDownLatch.countDown();
        }
    }

    private static class TimeoutSetter implements MemcacheSetter {

        @Override
        public void set(String key, int exp, Object value) {
            cache.put(key, value);
        }

        @Override
        public void syncSet(String key, int exp, Object value) {
            try {
                Thread.sleep(300);
            } catch (Exception ex) {

            }
            log.info("---syncset----" + key);
            cache.put(key, value);
        }
    }

    @Test
    public void testSet() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        MemcacheSetProcessor setProcessor = MemcacheSetProcessor.newBuilder()
                .setBufferSize(10)
                .setSetters(new MemcacheSetter[]{new TestMemcacheSetter(countDownLatch)})
                .build();
        setProcessor.set("1", 1000, "a");
        countDownLatch.await(3, TimeUnit.SECONDS);
        Assert.assertEquals(cache.get("1"), "a");
    }

    @Test
    public void testSyncSet() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        MemcacheSetProcessor setProcessor = MemcacheSetProcessor.newBuilder()
                .setSetters(new MemcacheSetter[]{new TestMemcacheSetter(countDownLatch)})
                .build();
        setProcessor.syncSet("2", 1000, "b");
        Assert.assertEquals(cache.get("2"), "b");
    }

    @Test
    public void testSetCallback() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        MemcacheSetProcessor setProcessor = MemcacheSetProcessor.newBuilder()
                .setSetters(new MemcacheSetter[]{new TestMemcacheSetter(null)})
                .build();
        setProcessor.set("1", 1000, "a", new MemcacheSetProcessor.SetCallback() {
            @Override
            public void complete(String key) {
                countDownLatch.countDown();
            }

            @Override
            public void fail(String key) {

            }

            @Override
            public void timeout(String key) {

            }
        });
        countDownLatch.await(3, TimeUnit.SECONDS);
        Assert.assertEquals(cache.get("1"), "a");
    }

    @Test
    public void testSetTimeout() throws Exception {
        final AtomicBoolean timeout = new AtomicBoolean(false);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        MemcacheSetProcessor setProcessor = MemcacheSetProcessor.newBuilder()
                .setBufferSize(2)
                .setAsync(false)
                .setSubmitTimeout(100, TimeUnit.MILLISECONDS)
                .setProcessorType(MemcacheProcessor.ProcessorType.QUEUE)
                .setSetters(new MemcacheSetter[]{new TimeoutSetter()})
                .build();
        MemcacheSetProcessor.SetCallback defaultCallback = new MemcacheSetProcessor.SetCallback() {

            @Override
            public void complete(String key) {

            }

            @Override
            public void fail(String key) {

            }

            @Override
            public void timeout(String key) {

            }
        };
        log.info("set----1");
        setProcessor.set("1", 1000, "a", defaultCallback);
        log.info("set----2");
        setProcessor.set("2", 1000, "b", defaultCallback);
        log.info("set----3");
        setProcessor.set("3", 1000, "b", defaultCallback);
        log.info("set----4");
        setProcessor.set("4", 1000, "c", new MemcacheSetProcessor.SetCallback() {

            @Override
            public void complete(String key) {

            }

            @Override
            public void fail(String key) {

            }

            @Override
            public void timeout(String key) {
                timeout.set(true);
                countDownLatch.countDown();
            }
        });
        log.info("set----5");
        countDownLatch.await(3, TimeUnit.SECONDS);
        Assert.assertTrue(timeout.get());
    }
}
