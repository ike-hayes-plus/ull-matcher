package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
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
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import org.junit.jupiter.api.Test;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherClusterSupervisorSmokeTest {
    @Test
    void supervisorRegistersLocalNodeAndFencesPrimaryAfterControlPlaneFailure() throws Exception {
        Path dir = Files.createTempDirectory("cluster-supervisor-smoke");
        ClusterTestDoubles.InMemoryLeaseStore leaseStore = new ClusterTestDoubles.InMemoryLeaseStore("node-a");
        ClusterTestDoubles.InMemoryNodeRegistry nodeRegistry = new ClusterTestDoubles.InMemoryNodeRegistry();
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
                new AeronTransportConfig(dir.resolve("aeron"), 15_190, 11_091),
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
                IngressAuthConfig.disabled(),
                19090,
                GrpcReplicationServerConfig.defaults(19090),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                MatchLoopConfig.defaults(),
                StandbySyncConfig.defaults(),
                io.github.ike.ullmatcher.server.orchestrator.OrchestratorRegistrationConfig.disabled(),
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

                nodeRegistry.failRegister = true;
                assertTrue(await(() -> nodeService.health().role() == HaRole.FENCED, 5_000L));
                assertTrue(await(() -> supervisor.metricsSnapshot().tickFailureCount() > 0L, 5_000L));
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
}
