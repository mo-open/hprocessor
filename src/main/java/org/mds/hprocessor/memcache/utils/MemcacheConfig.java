package org.mds.hprocessor.memcache.utils;

/**
 * Created by Randall.mo on 14-7-16.
 */
public class MemcacheConfig<T extends MemcacheConfig<T>> {
    private String servers;
    private int connections = 1;
    private int compressionThreshold = 1024;
    private int connectionTimeout = 3000;
    private int opTimeout = 2000;

    public MemcacheConfig(String servers) {
        this.servers = servers;
    }

    public String getServers() {
        return servers;
    }

    public T setServers(String servers) {
        this.servers = servers;
        return (T)this;
    }

    public int getConnections() {
        return connections;
    }

    public T setConnections(int connections) {
        this.connections = connections;
        return (T)this;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public T setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
        return (T)this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public T setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return (T)this;
    }

    public int getOpTimeout() {
        return opTimeout;
    }

    public T setOpTimeout(int opTimeout) {
        this.opTimeout = opTimeout;
        return (T)this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("servers=").append(this.servers)
                .append(",connections=").append(this.connections)
                .append(",connectionTimeout=").append(this.connectionTimeout)
                .append(",opTimeout=").append(this.opTimeout)
                .append(",compressionThreshold=").append(this.compressionThreshold);
        return stringBuilder.toString();
    }
}
