package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.readiness.PromotionReadinessPolicy;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.runtime.MatchLoopConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.junit.jupiter.api.Test;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherClusterSupervisorSmokeTest {
    @Test
    void supervisorRegistersLocalNodeAndTicksWithoutErrors() throws Exception {
        Path dir = Files.createTempDirectory("cluster-supervisor-smoke");
        InMemoryLeaseStore leaseStore = new InMemoryLeaseStore("node-a");
        InMemoryNodeRegistry nodeRegistry = new InMemoryNodeRegistry();
        MatcherClusterConfig clusterConfig = new MatcherClusterConfig(
                leaseStore,
                nodeRegistry,
                "merchant:42",
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
                new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 15_190, 11_091),
                ReplicationTransportPolicyConfig.defaults()
        );
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "merchant:42",
                MatcherConfig.defaults(1),
                dir.resolve("wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                dir.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 10,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                0,
                "127.0.0.1",
                2,
                1 << 20,
                256,
                2_000L,
                128,
                96,
                16,
                2_000L,
                1_000L,
                5_000L,
                96,
                64,
                2,
                16,
                8,
                WriteAdmissionPolicyConfig.defaults(),
                false,
                19090,
                GrpcReplicationServerConfig.defaults(19090),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                MatchLoopConfig.defaults(),
                StandbySyncConfig.defaults(),
                clusterConfig
        );
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            try (MatcherClusterSupervisor supervisor = new MatcherClusterSupervisor(
                    config,
                    nodeService,
                    new GrpcReplicationTransportProvider(ServerSecurityConfig.insecureDefaults()))) {
                assertTrue(await(() -> supervisor.metricsSnapshot().tickCount() > 0L, 5_000L));
                assertTrue(await(() -> nodeRegistry.listNodes().size() == 1, 5_000L));
                assertEquals(0L, supervisor.metricsSnapshot().tickFailureCount());
                assertEquals(1, nodeRegistry.listNodes().size());
                assertEquals("node-a", nodeRegistry.listNodes().getFirst().nodeId());
            }
        }
    }

    private static boolean await(Check check, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (check.ok()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    private static final class InMemoryNodeRegistry implements NodeRegistry {
        private final Map<String, DiscoveredNode> nodes = new LinkedHashMap<>();

        @Override
        public synchronized void registerOrUpdate(DiscoveredNode node) {
            nodes.put(node.nodeId(), node);
        }

        @Override
        public synchronized void unregister(String nodeId) {
            nodes.remove(nodeId);
        }

        @Override
        public synchronized List<DiscoveredNode> listNodes() {
            return new ArrayList<>(nodes.values());
        }
    }

    private static final class InMemoryLeaseStore implements LeaseStore {
        private volatile ClusterLease lease;

        private InMemoryLeaseStore(String ownerNodeId) {
            this.lease = new ClusterLease(ownerNodeId, new FencingToken(1L), System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
        }

        @Override
        public ClusterLease currentLease() {
            return lease;
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
            return true;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
            return true;
        }
    }
}
