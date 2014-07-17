package org.mds.hprocessor.memcache;

import net.rubyeye.xmemcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * Created by Randall.mo on 14-6-9.
 */
public class XMemcacheGetter implements MemcacheGetter {
    private static Logger log = LoggerFactory.getLogger(XMemcacheGetter.class);
    private MemcachedClient memcachedClient;

    public static XMemcacheGetter[] buildGetters(MemcacheConfig config, int count) {
        XMemcacheGetter[] getters = new XMemcacheGetter[count];
        for (int i = 0; i < count; i++) {
            getters[i] = new XMemcacheGetter(config);
        }
        return getters;
    }

    public XMemcacheGetter(MemcacheConfig config) {
        this.memcachedClient = MemcacheClientUtils.createXMemcachedClient(config);
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
            return this.memcachedClient.get(keys);
        } catch (Exception ex) {
            log.error("Failed to get {} from memcached", keys, ex);
            return null;
        }
    }
}
