package org.mds.hprocessor.memcache;

/**
 * Created by Randall.mo on 14-7-16.
 */
public class MemcacheConfig {
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

    public MemcacheConfig setServers(String servers) {
        this.servers = servers;
        return this;
    }

    public int getConnections() {
        return connections;
    }

    public MemcacheConfig setConnections(int connections) {
        this.connections = connections;
        return this;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public MemcacheConfig setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public int getOpTimeout() {
        return opTimeout;
    }

    public MemcacheConfig setOpTimeout(int opTimeout) {
        this.opTimeout = opTimeout;
        return this;
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
