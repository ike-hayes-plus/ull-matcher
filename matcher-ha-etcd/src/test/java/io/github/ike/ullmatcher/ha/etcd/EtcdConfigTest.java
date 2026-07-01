package io.github.ike.ullmatcher.ha.etcd;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class EtcdConfigTest {
    @Test
    void defaultsUseClusterScopedPrefixAndHotPathCache() {
        EtcdConfig config = EtcdConfig.defaults("http://127.0.0.1:2379", "cluster-a");

        assertEquals("http://127.0.0.1:2379", config.endpoint());
        assertEquals("/ull-matcher/cluster-a", config.keyPrefix());
        assertEquals(10L, config.leaseTtlSeconds());
        assertEquals(2_000L, config.timeoutMillis());
        assertEquals(25L, config.localHeldCheckCacheMillis());
    }

    @Test
    void rejectsInvalidControlPlaneConfig() {
        assertThrows(IllegalArgumentException.class, () -> new EtcdConfig("", "/ull", 10L, 1_000L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new EtcdConfig("http://127.0.0.1:2379", "relative", 10L, 1_000L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new EtcdConfig("http://127.0.0.1:2379", "/ull", 0L, 1_000L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new EtcdConfig("http://127.0.0.1:2379", "/ull", 10L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new EtcdConfig("http://127.0.0.1:2379", "/ull", 10L, 1_000L, -1L));
    }

    @Test
    void etcdClientCodecRoundTripsUtf8KeysAndValues() {
        String value = "/ull-matcher/集群-a/node-a";

        assertEquals(value, EtcdClient.decode(EtcdClient.encode(value)));
    }
}
