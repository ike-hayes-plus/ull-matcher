package io.github.ike.ullmatcher.discovery.zookeeper;

import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ZooKeeperNodeRegistryIntegrationTest {
    @Test
    void registerUpdateListAndUnregisterAgainstRealZooKeeper() throws Exception {
        try (TestingServer server = new TestingServer();
             ZooKeeperNodeRegistry registry = new ZooKeeperNodeRegistry(config(server))) {
            DiscoveredNode initial = new DiscoveredNode(
                    "node-a",
                    "10.0.0.11",
                    9090,
                    HaRole.STANDBY,
                    Map.of("shardKey", "symbol-1", "zone", "az-a")
            );
            registry.registerOrUpdate(initial);

            List<DiscoveredNode> listed = registry.listNodes();
            assertEquals(1, listed.size());
            assertEquals(initial, listed.get(0));

            DiscoveredNode updated = new DiscoveredNode(
                    "node-a",
                    "10.0.0.12",
                    9091,
                    HaRole.PRIMARY,
                    Map.of("shardKey", "symbol-1", "zone", "az-b")
            );
            registry.registerOrUpdate(updated);

            listed = registry.listNodes();
            assertEquals(1, listed.size());
            assertEquals(updated, listed.get(0));

            registry.unregister("node-a");
            assertTrue(registry.listNodes().isEmpty());
            registry.unregister("node-a");
        }
    }

    @Test
    void closingRegistryRemovesEphemeralNode() throws Exception {
        try (TestingServer server = new TestingServer()) {
            ZooKeeperNodeRegistry first = new ZooKeeperNodeRegistry(config(server));
            first.registerOrUpdate(new DiscoveredNode("node-a", "10.0.0.11", 9090, HaRole.STANDBY, Map.of()));
            assertEquals(1, first.listNodes().size());
            first.close();

            try (ZooKeeperNodeRegistry second = new ZooKeeperNodeRegistry(config(server))) {
                assertTrue(second.listNodes().isEmpty());
            }
        }
    }

    private static ZooKeeperDiscoveryConfig config(TestingServer server) {
        return new ZooKeeperDiscoveryConfig(
                server.getConnectString(),
                "/ull-matcher/test/discovery/nodes",
                5_000,
                5_000
        );
    }
}
