package org.mds.hprocessor.memcache.utils;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator;
import net.rubyeye.xmemcached.transcoders.SerializingTranscoder;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import org.glassfish.grizzly.memcached.MemcachedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall.mo
 */
public class MemcacheClientUtils {
    protected final static Logger log = LoggerFactory.getLogger(MemcacheClientUtils.class);

    public static MemcachedClient createXMemcachedClient(MemcacheConfig config) {
        try {
            XMemcachedClientBuilder builder = new XMemcachedClientBuilder(config.getServers());
            builder.setSessionLocator(new KetamaMemcachedSessionLocator());
            builder.setCommandFactory(new BinaryCommandFactory());
            builder.setConnectionPoolSize(config.getConnections());
            SerializingTranscoder transcoder = new SerializingTranscoder();
            transcoder.setCompressionThreshold(config.getCompressionThreshold());
            builder.setTranscoder(transcoder);
            builder.setConnectTimeout(config.getConnectionTimeout());
            builder.setFailureMode(false);
            builder.setOpTimeout(config.getOpTimeout());
            return builder.build();
        } catch (Exception ex) {
            log.error("Failed to create XMemcachedClient", ex);
        }
        return null;
    }

    public static net.spy.memcached.MemcachedClient[] createSpyMemcachedClients(MemcacheConfig config, int count) {
        net.spy.memcached.MemcachedClient[] clients = new net.spy.memcached.MemcachedClient[count];
        for (int i = 0; i < count; i++) {
            clients[i] = createSpyMemcachedClient(config);
        }
        return clients;
    }

    public static net.spy.memcached.MemcachedClient createSpyMemcachedClient(MemcacheConfig config) {
        try {
            ConnectionFactoryBuilder factoryBuilder = new ConnectionFactoryBuilder();

            net.spy.memcached.transcoders.SerializingTranscoder transcoder = new net.spy.memcached.transcoders.SerializingTranscoder();
            transcoder.setCompressionThreshold(config.getCompressionThreshold());

            factoryBuilder.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY);
            factoryBuilder.setTranscoder(transcoder);
            factoryBuilder.setOpTimeout(config.getOpTimeout());
            factoryBuilder.setTimeoutExceptionThreshold(5);
            factoryBuilder.setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT);
            factoryBuilder.setFailureMode(FailureMode.Redistribute);
            factoryBuilder.setUseNagleAlgorithm(false);
            factoryBuilder.setHashAlg(DefaultHashAlgorithm.KETAMA_HASH);

            return new net.spy.memcached.MemcachedClient(factoryBuilder.build(), AddrUtil.getAddresses(config.getServers()));
        } catch (Exception ex) {
            log.error("Failed to create SpyMemcachedClient", ex);
        }
        return null;
    }

    public static MemcachedCache<String, Object> createGrizzlyMemecache(GrizzlyMemcacheConfig memcacheConfig) {
        try {
            GrizzlyMemcacheManager.init(memcacheConfig);
            return GrizzlyMemcacheManager.build(memcacheConfig.getCacheName());
        } catch (Exception ex) {

        }
        return null;
    }
}
