package org.mds.hprocessor.memcache;

import java.util.Map;
import java.util.Set;

/**
 * Created by modongsong on 14-7-30.
 */
public class MemCacheAdapter implements MemCache {
    @Override
    public Object get(String key) {
        return null;
    }

    @Override
    public Map<String, Object> getMulti(Set<String> keys) {
        return null;
    }

    @Override
    public void set(String key, int exp, Object value) {

    }

    @Override
    public void setMulti(Map<String, Object> values, int exp) {

    }

    @Override
    public void syncSet(String key, int exp, Object value) {

    }

    @Override
    public void delete(String key) {

    }

    @Override
    public void syncDelete(String key) {

    }

    @Override
    public void delMulti(Set<String> keys) {

    }

    @Override
    public boolean supportMultiSet() {
        return false;
    }

    @Override
    public boolean supportMultiDel() {
        return false;
    }
}
