package org.mds.hprocessor.memcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.testng.Assert.*;

/**
 * Created by Randall.mo on 14-6-9.
 */
public class MemcacheGetProcessorTest {
    private static final Logger log = LoggerFactory.getLogger(MemcacheGetProcessorTest.class);
    private static Map<String, Object> cache = new HashMap();

    private static class TestMemcacheGetter implements MemcacheGetter {

        @Override
        public Object get(String key) {
            log.info("get ---0-- " + key);
            return cache.get(key);
        }

        @Override
        public Map<String, Object> getBulk(Collection<String> keys) {
            Map<String, Object> values = new HashMap();
            for (String key : keys) {
                values.put(key, cache.get(key));
            }
            return values;
        }
    }

    private static class TimeoutGetter extends TestMemcacheGetter {
        @Override
        public Map<String, Object> getBulk(Collection<String> keys) {
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {

            }
            Map<String, Object> values = new HashMap();
            for (String key : keys) {
                values.put(key, cache.get(key));
            }
            return values;
        }
    }

    private void get(MemcacheGetProcessor getProcessor, final boolean forTimeout) throws Exception {
        cache.put("1", "a");
        Assert.assertEquals(getProcessor.get("1"), "a");
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicBoolean timeout = new AtomicBoolean(false);
        getProcessor.get("1", null);
        getProcessor.get("2", null);
        getProcessor.get("2", new MemcacheGetProcessor.GetCallback() {
            @Override
            public void handle(String key, Object value) {
                log.info("9--------" + value);
            }

            @Override
            public void timeout(String key) {
                timeout.set(true);
                countDownLatch.countDown();
            }

            @Override
            public void handleNull(String key) {
                counter.incrementAndGet();
                countDownLatch.countDown();
            }
        });
        countDownLatch.await(3, TimeUnit.SECONDS);
        if (!forTimeout)
            Assert.assertTrue(counter.get() > 0);
        else
            Assert.assertTrue(timeout.get());
    }

    @Test
    public void testDisruptorGet() throws Exception {
        MemcacheGetProcessor getProcessor = MemcacheGetProcessor.newBuilder()
                .setProcessorType(MemcacheGetProcessor.ProcessorType.DISRUPTOR)
                .setBatchSize(10).setGetters(new TestMemcacheGetter[]{new TestMemcacheGetter()})
                .build();
        get(getProcessor, false);
    }

    @Test
    public void testQueueGet() throws Exception {
        MemcacheGetProcessor getProcessor = MemcacheGetProcessor.newBuilder()
                .setProcessorType(MemcacheGetProcessor.ProcessorType.QUEUE)
                .setBatchSize(10).setGetters(new TestMemcacheGetter[]{new TestMemcacheGetter()})
                .build();
        get(getProcessor, false);
    }

    @Test
    public void testGetTimeout() throws Exception {
        MemcacheGetProcessor getProcessor = MemcacheGetProcessor.newBuilder()
                .setBufferSize(2)
                .setGetTimeout(100, TimeUnit.MILLISECONDS)
                .setProcessorType(MemcacheGetProcessor.ProcessorType.QUEUE)
                .setBatchSize(10).setGetters(new TestMemcacheGetter[]{new TimeoutGetter()})
                .build();
        get(getProcessor, true);
    }

    @Test
    public void testDGetTimeout() throws Exception {
        MemcacheGetProcessor getProcessor = MemcacheGetProcessor.newBuilder()
                .setBufferSize(2)
                .setGetTimeout(100, TimeUnit.MILLISECONDS)
                .setProcessorType(MemcacheGetProcessor.ProcessorType.DISRUPTOR)
                .setBatchSize(10).setGetters(new TestMemcacheGetter[]{new TimeoutGetter()})
                .build();
        get(getProcessor, true);
    }

    @Test
    public void testGetWithCache1() throws Exception {
        MemcacheCache memcacheCache = new MemcacheCache(
                new CacheConfig().setCachedInTime(true).setSyncInterval(50));
        MemcacheGetProcessor getProcessor = MemcacheGetProcessor.newBuilder()
                .setProcessorType(MemcacheGetProcessor.ProcessorType.QUEUE)
                .setBatchSize(10).setGetters(new TestMemcacheGetter[]{new TestMemcacheGetter()})
                .setCache(memcacheCache)
                .build();
        assertNull(getProcessor.get("a"));
        cache.put("a", "1");
        try{
            Thread.sleep(150);
        }catch (Exception ex){

        }
        assertNotNull(getProcessor.get("a"));
    }

    @Test
    public void testGetWithCache2() throws Exception {
        MemcacheCache memcacheCache = new MemcacheCache(
                new CacheConfig().setCachedInTime(true).setSyncInterval(50));
        MemcacheGetProcessor getProcessor = MemcacheGetProcessor.newBuilder()
                .setProcessorType(MemcacheGetProcessor.ProcessorType.QUEUE)
                .setBatchSize(10).setGetters(new TestMemcacheGetter[]{new TestMemcacheGetter()})
                .setCache(memcacheCache)
                .build();
        getProcessor.get("a",null);
        cache.put("a", "1");
        try{
            Thread.sleep(250);
        }catch (Exception ex){

        }
        assertNotNull(getProcessor.get("a"));
    }
}
