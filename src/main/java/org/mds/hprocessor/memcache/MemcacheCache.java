package org.mds.hprocessor.memcache;

import com.google.common.base.Preconditions;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Randall.mo on 14-7-16.
 */
public class MemcacheCache {
    private final static Logger log = LoggerFactory.getLogger(MemcacheCache.class);
    private static Map<String, Long> keyMap = new NonBlockingHashMap<String, Long>();
    private static Map<String, Object> localCache = new NonBlockingHashMap<>();
    private ScheduledExecutorService mainExecutor;
    private ExecutorService syncExecutor;
    private CacheConfig cacheConfig;
    private volatile MemCache memcacheGetter;
    private volatile int currentKeysSize;
    private AtomicInteger timeoutCounter = new AtomicInteger(0);
    private int maxCachedCount;
    private List<CacheFilter> filters = new ArrayList<>();

    public MemcacheCache(CacheConfig cacheConfig) {
        Preconditions.checkArgument(cacheConfig != null, "cacheConfig can not be null");
        this.cacheConfig = cacheConfig;
        this.mainExecutor = Executors.newScheduledThreadPool(1);
        this.syncExecutor = Executors.newFixedThreadPool(cacheConfig.getSyncThreads());
        this.maxCachedCount = cacheConfig.getMaxCachedCount();
        this.mainExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (memcacheGetter != null)
                    doDataSync();
                else
                    log.warn("No MemcacheGetter.");
            }
        }, 0, cacheConfig.getSyncInterval(), TimeUnit.MILLISECONDS);
    }

    public MemcacheCache setMemcacheGetter(MemCache memcacheGetter) {
        this.memcacheGetter = memcacheGetter;
        return this;
    }

    public MemcacheCache setCacheFilter(CacheFilter cacheFilter) {
        Preconditions.checkArgument(cacheFilter != null, "cacheFilter can not be null");
        this.filters.add(cacheFilter);
        return this;
    }

    public Object get(String key) {
        if (this.maxCachedCount > 0) {
            boolean isContained = true;
            if (this.currentKeysSize < this.maxCachedCount) {
                isContained = this.keyMap.put(key, System.currentTimeMillis()) != null;
            }

            if (isContained) {
                Object value = this.localCache.get(key);
                for (CacheFilter filter : this.filters) {
                    if (!filter.filter(key, value)) {
                        this.keyMap.put(key, 0L);
                        break;
                    }
                }
                return value;
            }
        }

        return null;
    }

    public void cache(String key, Object value) {
        if (value == null || !this.cacheConfig.isCachedInTime()) return;
        for (CacheFilter filter : this.filters) {
            if (!filter.filter(key, value)) {
                return;
            }
        }

        this.keyMap.put(key, System.currentTimeMillis());
        this.localCache.put(key, value);
    }

    public void close() {
        if (this.syncExecutor != null) {
            this.syncExecutor.shutdownNow();
        }
        if (this.mainExecutor != null) {
            this.mainExecutor.shutdownNow();
        }
    }

    private void removeCacheItem(String key) {
        keyMap.remove(key);
        localCache.remove(key);
    }

    private void doDataSync() {
        try {
            long startTime = System.currentTimeMillis();
            List<Future<Integer>> tasks = new ArrayList<>();
            final AtomicInteger counter = new AtomicInteger(0);

            this.currentKeysSize = this.keyMap.size();
            if (this.currentKeysSize > 0) {
                Set<String> keys = new HashSet<>();
                for (Map.Entry<String, Long> entry : keyMap.entrySet()) {
                    if (System.currentTimeMillis() - entry.getValue() > this.cacheConfig.getMaxCachedTime()) {
                        removeCacheItem(entry.getKey());
                        continue;
                    }
                    keys.add(entry.getKey());
                    if (keys.size() == this.cacheConfig.getSyncBulkSize()) {
                        counter.addAndGet(this.cacheConfig.getSyncBulkSize());
                        tasks.add(this.syncExecutor.submit(new SyncTask(keys)));
                        keys = new HashSet<>();
                    }
                }
                if (!keys.isEmpty()) {
                    counter.addAndGet(keys.size());
                    tasks.add(this.syncExecutor.submit(new SyncTask(keys)));
                }
            }
            this.currentKeysSize = this.keyMap.size();
            int syncedCount = 0;
            for (Future<Integer> task : tasks) {
                try {
                    syncedCount = syncedCount + task.get();
                } catch (Exception ex) {

                }
            }
            if (log.isDebugEnabled()) {
                log.debug("Expected sync data:" + counter.get() +
                        ", Synced data:" + syncedCount + ", spent time: " +
                        (System.currentTimeMillis() - startTime));
            }
        } catch (Exception ex) {

        }
    }

    private class SyncTask implements Callable {
        private Set<String> keys;

        public SyncTask(Set<String> keys) {
            this.keys = keys;
        }

        @Override
        public Integer call() {
            try {
                Map<String, Object> values = MemcacheCache.this.memcacheGetter.getMulti(this.keys);
                localCache.putAll(values);
                this.keys.removeAll(values.keySet());
                for (String key : this.keys) {
                    removeCacheItem(key);
                }
                return values.size();
            } catch (Exception ex) {
                log.warn("Failed to do cache sync:" + ex.getMessage());
                int timeoutTimes = timeoutCounter.incrementAndGet();
                if (timeoutTimes > cacheConfig.getTimeoutThreshold()) {
                    for (String key : this.keys) {
                        removeCacheItem(key);
                    }
                }
                return 0;
            }
        }
    }

    public interface CacheFilter {
        public boolean filter(String key, Object value);
    }
}
