package org.mds.hprocessor.memcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * Created by Randall.mo on 14-7-22.
 */
public class MemcacheCacheTest {
    private static final Logger log = LoggerFactory.getLogger(MemcacheCacheTest.class);
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

    @Test
    public void testGet() throws Exception {
        cache.put("b", "1");
        MemcacheCache memcacheCache = new MemcacheCache(
                new CacheConfig().setCachedInTime(false).setSyncInterval(50));
        MemcacheGetter memcacheGetter = new TestMemcacheGetter();
        memcacheCache.setMemcacheGetter(memcacheGetter);
        assertNull(memcacheCache.get("b"));

        try {
            Thread.sleep(250);
        } catch (Exception ex) {

        }
        assertEquals(memcacheCache.get("b"), "1");
        memcacheCache.close();
    }

    @Test
    public void testCachedInTime() throws Exception {
        MemcacheCache memcacheCache = new MemcacheCache(
                new CacheConfig().setCachedInTime(true));
        MemcacheGetter memcacheGetter = new TestMemcacheGetter();
        memcacheCache.setMemcacheGetter(memcacheGetter);
        assertNull(memcacheCache.get("a"));
        memcacheCache.cache("a", "1");
        assertEquals(memcacheCache.get("a"), "1");
        memcacheCache.close();
    }

    @Test
    public void testCacheFilter() throws Exception {
        cache.put("c", "1");
        MemcacheCache memcacheCache = new MemcacheCache(
                new CacheConfig().setCachedInTime(false).setSyncInterval(50));
        MemcacheGetter memcacheGetter = new TestMemcacheGetter();
        memcacheCache.setMemcacheGetter(memcacheGetter);
        memcacheCache.setCacheFilter(new MemcacheCache.CacheFilter() {
            @Override
            public boolean filter(String key, Object value) {
                return false;
            }
        });

        assertNull(memcacheCache.get("c"));
        try {
            Thread.sleep(250);
        } catch (Exception ex) {

        }
        assertEquals(memcacheCache.get("c"), "1");

        try {
            Thread.sleep(250);
        } catch (Exception ex) {

        }
        assertNull(memcacheCache.get("c"));
        memcacheCache.close();
    }
}
