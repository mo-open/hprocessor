package org.mds.hprocessor.memcache;

/**
 * Created by Randall.mo on 14-6-9.
 */
public interface MemcacheSetter {
    public void set(String key, int exp, Object value);
    public void syncSet(String key, int exp, Object value);
}
