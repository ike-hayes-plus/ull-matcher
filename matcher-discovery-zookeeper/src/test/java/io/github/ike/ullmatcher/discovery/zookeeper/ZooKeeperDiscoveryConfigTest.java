package io.github.ike.ullmatcher.discovery.zookeeper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ZooKeeperDiscoveryConfigTest {
    @Test
    void defaultsUseClusterScopedNodePath() {
        ZooKeeperDiscoveryConfig config = ZooKeeperDiscoveryConfig.defaults("127.0.0.1:2181", "cluster-a");

        assertEquals("127.0.0.1:2181", config.connectString());
        assertEquals("/ull-matcher/discovery/cluster-a/nodes", config.servicePath());
        assertEquals(15_000, config.sessionTimeoutMillis());
        assertEquals(5_000, config.connectionTimeoutMillis());
    }

    @Test
    void rejectsInvalidDiscoveryConfig() {
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperDiscoveryConfig("", "/ull/nodes", 15_000, 5_000));
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperDiscoveryConfig("127.0.0.1:2181", "", 15_000, 5_000));
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperDiscoveryConfig("127.0.0.1:2181", "relative", 15_000, 5_000));
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperDiscoveryConfig("127.0.0.1:2181", "/ull/nodes", 0, 5_000));
        assertThrows(IllegalArgumentException.class, () -> new ZooKeeperDiscoveryConfig("127.0.0.1:2181", "/ull/nodes", 15_000, 0));
    }
}
