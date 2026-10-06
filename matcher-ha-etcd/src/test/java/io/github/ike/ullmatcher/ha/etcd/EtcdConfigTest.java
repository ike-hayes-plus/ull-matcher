package io.github.ike.ullmatcher.ha.etcd;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EtcdConfigTest {
    @Test
    void leaseGrantTtlRoundsNanosUpToWholeSeconds() {
        assertEquals(5L, EtcdLeaseStore.grantTtlSeconds(5_000_000_000L));
        assertEquals(1L, EtcdLeaseStore.grantTtlSeconds(1L));
        assertEquals(2L, EtcdLeaseStore.grantTtlSeconds(1_000_000_001L));
        assertThrows(IllegalArgumentException.class, () -> EtcdLeaseStore.grantTtlSeconds(0L));
    }

    @Test
    void defaultsUseClusterScopedPrefix() {
        EtcdConfig config = EtcdConfig.defaults("http://127.0.0.1:2379", "cluster-a");

        assertEquals("http://127.0.0.1:2379", config.endpoint());
        assertEquals("/ull-matcher/cluster-a", config.keyPrefix());
        assertEquals(10L, config.leaseTtlSeconds());
        assertEquals(2_000L, config.timeoutMillis());
        assertFalse(config.enforceProductionSafety());
    }

    @Test
    void rejectsInvalidControlPlaneConfig() {
        assertThrows(IllegalArgumentException.class, () -> config("", "/ull", 10L, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> config("http://127.0.0.1:2379", "relative", 10L, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> config("http://127.0.0.1:2379", "/ull", 0L, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> config("http://127.0.0.1:2379", "/ull", 10L, 0L));
    }

    @Test
    void rejectsHalfConfiguredMutualTls() {
        assertThrows(IllegalArgumentException.class, () -> new EtcdConfig(
                "http://127.0.0.1:2379", "/ull", 10L, 1_000L, null, Path.of("cert.pem"), null, false));
        assertThrows(IllegalArgumentException.class, () -> new EtcdConfig(
                "http://127.0.0.1:2379", "/ull", 10L, 1_000L, null, null, Path.of("key.pem"), false));
    }

    @Test
    void prodSafetyRejectsPlainHttpRemoteEndpoint() {
        EtcdConfig config = config("http://10.0.0.10:2379", "/ull-matcher/test", 10L, 1_000L);
        assertThrows(IllegalStateException.class, config::validateProductionSafety);
    }

    @Test
    void prodSafetyIsEnforcedAtConstructionTime() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> new EtcdConfig(
                "http://10.0.0.10:2379", "/ull-matcher/test", 10L, 1_000L, null, null, null, true));

        assertTrue(error.getMessage().contains("https"), error.getMessage());
    }

    @Test
    void prodSafetyRejectsPlainHttpInAnyEndpointOfTheList() {
        assertThrows(IllegalStateException.class, () -> new EtcdConfig(
                "https://10.0.0.10:2379,http://10.0.0.11:2379", "/ull", 10L, 1_000L, null, null, null, true));
    }

    @Test
    void prodSafetyAllowsLoopbackAndHttps() {
        assertTrue(new EtcdConfig("http://127.0.0.1:2379", "/ull", 10L, 1_000L, null, null, null, true)
                .enforceProductionSafety());
        assertTrue(new EtcdConfig("http://localhost:2379", "/ull", 10L, 1_000L, null, null, null, true)
                .enforceProductionSafety());
        assertTrue(new EtcdConfig("https://etcd.internal:2379", "/ull", 10L, 1_000L, null, null, null, true)
                .enforceProductionSafety());
    }

    @Test
    void withProductionSafetyValidatesLazilyAndIsIdempotent() {
        EtcdConfig insecure = config("http://10.0.0.10:2379", "/ull", 10L, 1_000L);
        assertThrows(IllegalStateException.class, insecure::withProductionSafety);

        EtcdConfig secure = config("https://etcd.internal:2379", "/ull", 10L, 1_000L).withProductionSafety();
        assertTrue(secure.enforceProductionSafety());
        assertEquals(secure, secure.withProductionSafety());
    }

    @Test
    void etcdClientCodecRoundTripsUtf8KeysAndValues() {
        String value = "/ull-matcher/集群-a/node-a";

        assertEquals(value, EtcdClient.decode(EtcdClient.encode(value)));
    }

    private static EtcdConfig config(String endpoint, String prefix, long ttl, long timeout) {
        return new EtcdConfig(endpoint, prefix, ttl, timeout, null, null, null, false);
    }
}
