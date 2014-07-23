package org.mds.hprocessor.memcache;

import java.util.Map;
import java.util.Set;

/**
 * Created by modongsong on 14-7-23.
 */
public interface MemCache {
    public Object get(String key);

    public Map<String, Object> getMulti(Set<String> keys);

    public void set(String key, int exp, Object value);

    public void setMulti(Map<String, Object> values, int exp);

    public void syncSet(String key, int exp, Object value);

    public void delete(String key);

    public void syncDelete(String key);

    public void delMulti(Set<String> keys);

    public boolean supportMultiSet();

    public boolean supportMultiDel();
}
