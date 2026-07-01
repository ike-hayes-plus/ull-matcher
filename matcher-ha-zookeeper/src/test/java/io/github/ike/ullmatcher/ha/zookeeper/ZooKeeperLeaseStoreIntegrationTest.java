package io.github.ike.ullmatcher.ha.zookeeper;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ZooKeeperLeaseStoreIntegrationTest {
    private static final long TTL_NANOS = TimeUnit.SECONDS.toNanos(10);

    @Test
    void acquireExtendAndRejectCompetingOwnerAgainstRealZooKeeper() throws Exception {
        try (TestingServer server = new TestingServer();
             ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(config(server))) {
            long nowNanos = System.nanoTime();

            assertNull(store.currentLease());
            assertTrue(store.tryAcquire("node-a", new FencingToken(1L), nowNanos, TTL_NANOS));

            ClusterLease lease = store.currentLease();
            assertNotNull(lease);
            assertEquals("node-a", lease.ownerNodeId());
            assertEquals(new FencingToken(1L), lease.fencingToken());
            assertFalse(lease.isExpired(System.nanoTime()));

            assertFalse(store.tryAcquire("node-b", new FencingToken(2L), nowNanos + 1L, TTL_NANOS));
            assertTrue(store.tryExtend("node-a", new FencingToken(1L), nowNanos + 2L, TTL_NANOS));
            assertFalse(store.tryExtend("node-a", new FencingToken(2L), nowNanos + 3L, TTL_NANOS));
            assertFalse(store.tryExtend("node-b", new FencingToken(1L), nowNanos + 4L, TTL_NANOS));

            ClusterLease extended = store.currentLease();
            assertNotNull(extended);
            assertEquals("node-a", extended.ownerNodeId());
            assertEquals(new FencingToken(1L), extended.fencingToken());
        }
    }

    @Test
    void closingOwningClientReleasesEphemeralLease() throws Exception {
        try (TestingServer server = new TestingServer()) {
            ZooKeeperLeaseStore first = new ZooKeeperLeaseStore(config(server));
            assertTrue(first.tryAcquire("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));
            first.close();

            try (ZooKeeperLeaseStore second = new ZooKeeperLeaseStore(config(server))) {
                assertTrue(second.tryAcquire("node-b", new FencingToken(2L), System.nanoTime(), TTL_NANOS));
                ClusterLease lease = second.currentLease();
                assertNotNull(lease);
                assertEquals("node-b", lease.ownerNodeId());
                assertEquals(new FencingToken(2L), lease.fencingToken());
            }
        }
    }

    private static ZooKeeperLeaseStoreConfig config(TestingServer server) {
        return new ZooKeeperLeaseStoreConfig(
                server.getConnectString(),
                "/ull-matcher/test/shard-a/lease",
                5_000,
                5_000
        );
    }
}
