package org.mds.hprocessor.memcache.utils;

import com.google.code.yanf4j.util.ConcurrentHashSet;
import com.google.common.base.Preconditions;
import org.glassfish.grizzly.memcached.GrizzlyMemcachedCache;
import org.glassfish.grizzly.memcached.GrizzlyMemcachedCacheManager;
import org.glassfish.grizzly.memcached.MemcachedCache;
import org.glassfish.grizzly.memcached.zookeeper.ZooKeeperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by modongsong on 14-8-4.
 */
public class GrizzlyMemcacheManager {
    protected final static Logger log = LoggerFactory.getLogger(GrizzlyMemcacheManager.class);
    private static Set<String> cacheNames = new ConcurrentHashSet<>();
    private static GrizzlyMemcachedCacheManager manager;
    private static volatile boolean isInited = false;
    private static boolean useZookeeper = true;
    private static GrizzlyMemcacheConfig memcacheConfig;

    public static synchronized void init(GrizzlyMemcacheConfig memcacheConfig) {
        if (isInited) return;
        isInited = true;
        GrizzlyMemcacheManager.memcacheConfig = memcacheConfig;
        final GrizzlyMemcachedCacheManager.Builder managerBuilder = new GrizzlyMemcachedCacheManager.Builder();
        // setup zookeeper server
        if (memcacheConfig.getZkHosts() == null || "".equals(memcacheConfig.getZkHosts())) {
            useZookeeper = false;
        }
        if (useZookeeper) {
            final ZooKeeperConfig zkConfig = ZooKeeperConfig.create(memcacheConfig.getZkConfName(), memcacheConfig.getZkHosts());
            zkConfig.setRootPath(memcacheConfig.getZkRoot());
            zkConfig.setConnectTimeoutInMillis(3000);
            zkConfig.setSessionTimeoutInMillis(30000);
            zkConfig.setCommitDelayTimeInSecs(3);
            managerBuilder.zooKeeperConfig(zkConfig);
        }
        // create a cache manager
        manager = managerBuilder.build();
    }

    public static synchronized MemcachedCache<String, Object> build(String cacheName) {
        Preconditions.checkArgument(isInited, "Please call init() to initialize firstly");
        GrizzlyMemcachedCache.Builder<String, Object> cacheBuilder = manager.createCacheBuilder(cacheName);
        if (!useZookeeper) {
            if (memcacheConfig.getServers() == null || "".equals(memcacheConfig.getServers())) {
                throw new IllegalArgumentException("No zookeeper and memcache servers configuration");
            }
            Set<SocketAddress> memcachedServers = new HashSet<SocketAddress>();
            for (String address : memcacheConfig.getZkHosts().split(",")) {
                try {
                    String[] hostPort = (address + ":").split(":");
                    memcachedServers.add(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
                } catch (Exception ex) {
                    log.error("Failed to set memcached server:" + ex);
                }
            }
            cacheBuilder.servers(memcachedServers);
        }
        cacheNames.add(cacheName);
        return cacheBuilder.maxConnectionPerServer(25).build();
    }

    public static synchronized void shutdown() {
        for (String cacheName : cacheNames) {
            manager.removeCache(cacheName);
        }
        manager.shutdown();
        isInited = false;
    }
}
