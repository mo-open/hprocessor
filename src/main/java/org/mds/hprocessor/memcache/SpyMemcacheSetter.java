package org.mds.hprocessor.memcache;

import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Randall.mo on 14-6-25.
 */
public class SpyMemcacheSetter implements MemcacheSetter {
    private static Logger log = LoggerFactory.getLogger(SpyMemcacheSetter.class);
    private MemcachedClient memcachedClient;

    public static SpyMemcacheSetter[] buildSetters(MemcacheConfig config, int count) {
        SpyMemcacheSetter[] setters = new SpyMemcacheSetter[count];
        for (int i = 0; i < count; i++) {
            setters[i] = new SpyMemcacheSetter(config);
        }
        return setters;
    }

    public SpyMemcacheSetter(MemcacheConfig config) {
        this.memcachedClient = MemcacheClientUtils.createSpyMemcachedClient(config);
    }

    @Override
    public void set(String key, int exp, Object value) {
        try {
            this.memcachedClient.set(key, exp, value);
        } catch (Exception ex) {
            log.error("Failed to set {} with value {}", key, value, ex);
        }
    }

    @Override
    public void syncSet(String key, int exp, Object value) {
        try {
            this.memcachedClient.set(key, exp, value).get();
        } catch (Exception ex) {
            log.error("Failed to sync set {} with value {}", key, value, ex);
        }
    }
}
