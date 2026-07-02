package io.github.ike.ullmatcher.ha.zookeeper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ZooKeeperLeaseStoreConfigTest {
    @Test
    void defaultsUseProductionTimeouts() {
        ZooKeeperLeaseStoreConfig config = ZooKeeperLeaseStoreConfig.of("127.0.0.1:2181", "/ull/lease");

        assertEquals("127.0.0.1:2181", config.connectString());
        assertEquals("/ull/lease", config.leasePath());
        assertEquals(15_000, config.sessionTimeoutMillis());
        assertEquals(5_000, config.connectionTimeoutMillis());
    }

    @Test
    void rejectsInvalidLeaseStoreConfig() {
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperLeaseStoreConfig("", "/ull/lease", 15_000, 5_000));
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperLeaseStoreConfig("127.0.0.1:2181", "relative", 15_000, 5_000));
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperLeaseStoreConfig("127.0.0.1:2181", "/ull/lease", 0, 5_000));
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperLeaseStoreConfig("127.0.0.1:2181", "/ull/lease", 15_000, 0));
    }
}
