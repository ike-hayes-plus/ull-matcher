package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.readiness.PromotionReadinessPolicy;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cluster-level contract: periodic and authoritative RDB run only on PRIMARY.
 */
final class MatcherNodeServiceClusterPeriodicSnapshotTest {
    @Test
    void standbyDoesNotWritePeriodicSnapshotWhilePrimaryDoes() throws Exception {
        Path root = Files.createTempDirectory("cluster-periodic-snapshot");
        Path primaryDir = root.resolve("primary");
        Path standbyDir = root.resolve("standby");
        ClusterTestDoubles.InMemoryLeaseStore leaseStore = new ClusterTestDoubles.InMemoryLeaseStore("node-a");
        ClusterTestDoubles.InMemoryNodeRegistry nodeRegistry = new ClusterTestDoubles.InMemoryNodeRegistry();
        MatcherClusterConfig cluster = clusterConfig(primaryDir, leaseStore, nodeRegistry);

        MatcherServerConfig primaryConfig = MatcherServerConfig.builder("node-a", 1, primaryDir)
                .initialRole(HaRole.PRIMARY)
                .clusterConfig(cluster)
                .snapshotIntervalMillis(200L)
                .build();
        MatcherServerConfig standbyConfig = MatcherServerConfig.builder("node-b", 1, standbyDir)
                .initialRole(HaRole.STANDBY)
                .clusterConfig(cluster)
                .snapshotIntervalMillis(200L)
                .build();

        try (MatcherNodeService primary = new MatcherNodeService(primaryConfig);
             MatcherNodeService standby = new MatcherNodeService(standbyConfig)) {
            primary.start();
            standby.start();
            assertEquals(HaRole.PRIMARY, primary.health().role());
            assertEquals(HaRole.STANDBY, standby.health().role());

            primary.submitNewOrder(1L, 1L, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 10L, null);

            assertTrue(awaitFile(primaryConfig.snapshotFile(), 5_000L),
                    "expected PRIMARY periodic snapshot file");
            assertTrue(primary.latestSnapshot().lastSequence() >= 1L);

            Thread.sleep(800L);
            assertEquals(HaRole.STANDBY, standby.health().role());
            assertFalse(Files.exists(standbyConfig.snapshotFile()),
                    "STANDBY must not write a periodic RDB");

            IOException latest = assertThrows(IOException.class, standby::latestSnapshot);
            assertTrue(latest.getMessage().contains("only primary can create or export an authoritative snapshot"));
            IOException created = assertThrows(IOException.class, standby::createSnapshot);
            assertTrue(created.getMessage().contains("only primary can create or export an authoritative snapshot"));
            assertFalse(Files.exists(standbyConfig.snapshotFile()),
                    "STANDBY must not write an RDB from latestSnapshot or admin createSnapshot");
        }
    }

    private static MatcherClusterConfig clusterConfig(Path dir, LeaseStore leaseStore, NodeRegistry nodeRegistry) {
        return new MatcherClusterConfig(
                leaseStore,
                nodeRegistry,
                "symbol-1",
                "127.0.0.1",
                25L,
                TimeUnit.MILLISECONDS.toNanos(100),
                TimeUnit.SECONDS.toNanos(5),
                FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(),
                0L,
                TimeUnit.SECONDS.toNanos(1),
                ReplicationMode.LOCAL_ONLY,
                TimeUnit.MILLISECONDS.toNanos(50),
                ReplicationTransportType.GRPC,
                new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 15_390, 11_291),
                ReplicationTransportPolicyConfig.defaults()
        );
    }

    private static boolean awaitFile(Path file, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.exists(file)) {
                return true;
            }
            Thread.sleep(50L);
        }
        return Files.exists(file);
    }
}
