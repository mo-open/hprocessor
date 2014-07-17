package org.mds.hprocessor.memcache;

import net.rubyeye.xmemcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Randall.mo on 14-6-9.
 */
public class XMemcacheSetter implements MemcacheSetter {
    private static Logger log = LoggerFactory.getLogger(XMemcacheSetter.class);
    private MemcachedClient memcachedClient;

    public static XMemcacheSetter[] buildSetters(MemcacheConfig config, int count) {
        XMemcacheSetter[] setters = new XMemcacheSetter[count];
        for (int i = 0; i < count; i++) {
            setters[i] = new XMemcacheSetter(config);
        }
        return setters;
    }

    public XMemcacheSetter(MemcacheConfig config) {
        this.memcachedClient = MemcacheClientUtils.createXMemcachedClient(config);
    }

    @Override
    public void set(String key, int exp, Object value) {
        try {
            this.memcachedClient.setWithNoReply(key, exp, value);
        } catch (Exception ex) {
            log.error("Failed to set {} with value {}", key, value, ex);
        }
    }

    @Override
    public void syncSet(String key, int exp, Object value) {
        try {
            this.memcachedClient.set(key, exp, value);
        } catch (Exception ex) {
            log.error("Failed to sync set {} with value {}", key, value, ex);
        }
    }
}
