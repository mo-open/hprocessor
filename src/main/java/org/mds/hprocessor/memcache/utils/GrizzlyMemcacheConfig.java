package org.mds.hprocessor.memcache.utils;

/**
 * Created by modongsong on 14-8-1.
 */
public class GrizzlyMemcacheConfig extends MemcacheConfig<GrizzlyMemcacheConfig> {
    private String cacheName = "Default";
    private String zkConfName = "cache-manager";
    private String zkHosts;
    private String zkRoot;

    public GrizzlyMemcacheConfig() {
        super("");
    }

    public String getCacheName() {
        return cacheName;
    }

    public GrizzlyMemcacheConfig setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    public String getZkConfName() {
        return zkConfName;
    }

    public GrizzlyMemcacheConfig setZkConfName(String zkConfName) {
        this.zkConfName = zkConfName;
        return this;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    public GrizzlyMemcacheConfig setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
        return this;
    }

    public String getZkRoot() {
        return zkRoot;
    }

    public GrizzlyMemcacheConfig setZkRoot(String zkRoot) {
        this.zkRoot = zkRoot;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("zkHosts=").append(this.zkHosts)
                .append(",zkRoot=").append(this.zkRoot)
                .append(",zkConfName=").append(this.zkConfName)
                .append(",cacheName=").append(this.cacheName).append(",");
        stringBuilder.append(super.toString());
        return stringBuilder.toString();
    }
}
