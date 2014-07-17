package org.mds.hprocessor.memcache;

import java.util.Collection;
import java.util.Map;

/**
 * @author Randall.mo
 */
public interface MemcacheGetter {
    public Object get(String key);

    public Map<String, Object> getBulk(Collection<String> keys);
}
