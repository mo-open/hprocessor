package org.mds.hprocessor.memcache;

/**
 * Created by Randall.mo on 14-7-16.
 */
public class CacheConfig {
    private int syncThreads = 1;
    private int timeoutThreshold = 10;
    private int maxCachedTime = 10000;
    private int syncBulkSize = 200;
    private int syncInterval = 3000;
    private int maxCachedCount = 30000;
    private boolean cachedInTime = false;

    public int getSyncThreads() {
        return syncThreads;
    }

    public CacheConfig setSyncThreads(int syncThreads) {
        this.syncThreads = syncThreads;
        return this;
    }

    public int getTimeoutThreshold() {
        return timeoutThreshold;
    }

    public CacheConfig setTimeoutThreshold(int timeoutThreshold) {
        this.timeoutThreshold = timeoutThreshold;
        return this;
    }

    public int getMaxCachedTime() {
        return maxCachedTime;
    }

    public CacheConfig setMaxCachedTime(int maxCachedTime) {
        this.maxCachedTime = maxCachedTime;
        return this;
    }

    public int getSyncBulkSize() {
        return syncBulkSize;
    }

    public CacheConfig setSyncBulkSize(int syncBulkSize) {
        this.syncBulkSize = syncBulkSize;
        return this;
    }

    public int getSyncInterval() {
        return syncInterval;
    }

    public CacheConfig setSyncInterval(int syncInterval) {
        this.syncInterval = syncInterval;
        return this;
    }

    public int getMaxCachedCount() {
        return maxCachedCount;
    }

    public CacheConfig setMaxCachedCount(int maxCachedCount) {
        this.maxCachedCount = maxCachedCount;
        return this;
    }

    public boolean isCachedInTime() {
        return cachedInTime;
    }

    public CacheConfig setCachedInTime(boolean cachedInTime) {
        this.cachedInTime = cachedInTime;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("maxCachedCount=").append(this.maxCachedCount)
                .append("maxCachedTime=").append(this.maxCachedTime)
                .append("syncThreads=").append(this.syncThreads)
                .append(",syncInterval=").append(this.syncInterval)
                .append(",syncBulkSize=").append(this.syncBulkSize)
                .append(",cachedInTime=").append(this.cachedInTime);

        return stringBuilder.toString();
    }
}
