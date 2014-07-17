package org.mds.hprocessor.memcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import net.spy.memcached.MemcachedClient;

/**
 * Created by Randall.mo on 14-6-25.
 */
public class SpyMemcacheGetter implements MemcacheGetter {
    private static Logger log = LoggerFactory.getLogger(SpyMemcacheSetter.class);
    private MemcachedClient memcachedClient;

    public static SpyMemcacheGetter[] buildGetters(MemcacheConfig config, int count) {
        SpyMemcacheGetter[] getters = new SpyMemcacheGetter[count];
        for (int i = 0; i < count; i++) {
            getters[i] = new SpyMemcacheGetter(config);
        }
        return getters;
    }

    public SpyMemcacheGetter(MemcacheConfig config) {
        this.memcachedClient = MemcacheClientUtils.createSpyMemcachedClient(config);
    }

    @Override
    public Object get(String key) {
        try {
            return this.memcachedClient.get(key);
        } catch (Exception ex) {
            log.error("Failed to get {} from memcached", key, ex);
            return null;
        }
    }

    @Override
    public Map<String, Object> getBulk(Collection<String> keys) {
        try {
            return this.memcachedClient.getBulk(keys);
        } catch (Exception ex) {
            log.error("Failed to get {} from memcached", keys, ex);
            return null;
        }
    }
}
