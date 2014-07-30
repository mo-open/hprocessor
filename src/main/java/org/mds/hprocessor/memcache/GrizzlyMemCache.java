package org.mds.hprocessor.memcache;

import net.rubyeye.xmemcached.MemcachedClient;
import org.glassfish.grizzly.memcached.MemcachedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Created by modongsong on 14-7-23.
 */
public class GrizzlyMemCache implements MemCache {
    private static Logger log = LoggerFactory.getLogger(GrizzlyMemCache.class);
    private MemcachedCache<String, Object> cache;

    public static GrizzlyMemCache[] build(MemcachedCache<String, Object> cache, int count) {
        GrizzlyMemCache[] caches = new GrizzlyMemCache[count];
        for (int i = 0; i < count; i++) {
            caches[i] = new GrizzlyMemCache(cache);
        }
        return caches;
    }

    public GrizzlyMemCache(MemcachedCache<String, Object> cache) {
        this.cache = cache;
    }

    @Override
    public Object get(String key) {
        return this.cache.get(key, false);
    }

    @Override
    public Map<String, Object> getMulti(Set<String> keys) {
        return this.cache.getMulti(keys);
    }

    @Override
    public void set(String key, int exp, Object value) {
        this.cache.set(key, value, exp, true);
    }

    @Override
    public void setMulti(Map<String, Object> values, int exp) {
        this.cache.setMulti(values, exp);
    }

    @Override
    public void syncSet(String key, int exp, Object value) {
        this.cache.set(key, value, exp, false);
    }

    @Override
    public void delete(String key) {
        this.cache.delete(key, true);
    }

    @Override
    public void syncDelete(String key) {
        this.cache.delete(key, false);
    }

    @Override
    public void delMulti(Set<String> keys) {
        this.cache.deleteMulti(keys);
    }

    @Override
    public boolean supportMultiSet() {
        return true;
    }

    @Override
    public boolean supportMultiDel() {
        return true;
    }
}
