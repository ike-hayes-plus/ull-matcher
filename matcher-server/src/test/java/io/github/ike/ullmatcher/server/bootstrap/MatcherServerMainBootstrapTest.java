package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.cluster.AeronPreviewTransportConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyEnforcer;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportType;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.junit.jupiter.api.Test;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class MatcherServerMainBootstrapTest {
    @Test
    void unsupportedDiscoveryProviderFailsFast() {
        ServerBootstrapException error = assertThrows(ServerBootstrapException.class,
                () -> MatcherServerMain.nodeRegistry("unsupported", "127.0.0.1:2181", "cluster-a"));
        assertEquals("unsupported discovery provider: unsupported", error.getMessage());
    }

    @Test
    void nacosProviderRequiresServerAddress() {
        String previous = System.getProperty("matcher.nacosServerAddr");
        try {
            System.clearProperty("matcher.nacosServerAddr");
            ServerBootstrapException error = assertThrows(ServerBootstrapException.class,
                    () -> MatcherServerMain.nacosDiscoveryConfig("cluster-a"));
            assertEquals("matcher.nacosServerAddr is required when matcher.discoveryProvider=nacos", error.getMessage());
        } finally {
            restoreProperty("matcher.nacosServerAddr", previous);
        }
    }

    @Test
    void nacosProviderUsesConfiguredOverrides() {
        String previousAddr = System.getProperty("matcher.nacosServerAddr");
        String previousService = System.getProperty("matcher.nacosServiceName");
        String previousGroup = System.getProperty("matcher.nacosGroup");
        String previousNamespace = System.getProperty("matcher.nacosNamespace");
        String previousCluster = System.getProperty("matcher.nacosClusterName");
        try {
            System.setProperty("matcher.nacosServerAddr", "127.0.0.1:8848");
            System.setProperty("matcher.nacosServiceName", "matcher-test");
            System.setProperty("matcher.nacosGroup", "MATCHER");
            System.setProperty("matcher.nacosNamespace", "dev");
            System.setProperty("matcher.nacosClusterName", "cluster-b");
            var config = MatcherServerMain.nacosDiscoveryConfig("cluster-a");
            assertEquals("127.0.0.1:8848", config.serverAddress());
            assertEquals("matcher-test", config.serviceName());
            assertEquals("MATCHER", config.groupName());
            assertEquals("dev", config.namespace());
            assertEquals("cluster-b", config.clusterName());
        } finally {
            restoreProperty("matcher.nacosServerAddr", previousAddr);
            restoreProperty("matcher.nacosServiceName", previousService);
            restoreProperty("matcher.nacosGroup", previousGroup);
            restoreProperty("matcher.nacosNamespace", previousNamespace);
            restoreProperty("matcher.nacosClusterName", previousCluster);
        }
    }

    @Test
    void prodModeRejectsRemoteHttpWithoutExplicitOptIn() {
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.PROD,
                "node-a",
                "merchant:42",
                MatcherConfig.defaults(1),
                Path.of("target/test-wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                Path.of("target/test.snap"),
                1 << 10,
                128,
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(200),
                8080,
                "10.0.0.10",
                4,
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
                9090,
                GrpcReplicationServerConfig.defaults(9090),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );
        IllegalStateException error = assertThrows(IllegalStateException.class, config::validateDeploymentSafety);
        assertEquals("prod mode requires matcher.allowInsecureRemoteHttp=true when matcher.httpBindHost is not loopback", error.getMessage());
    }

    @Test
    void prodModeRejectsRemoteGrpcWithoutTls() {
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.PROD,
                "node-a",
                "merchant:42",
                MatcherConfig.defaults(1),
                Path.of("target/test-wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                Path.of("target/test.snap"),
                1 << 10,
                128,
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(200),
                8080,
                "127.0.0.1",
                4,
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
                9090,
                GrpcReplicationServerConfig.defaults(9090).withBindHost("10.0.0.10"),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );
        IllegalStateException error = assertThrows(IllegalStateException.class, config::validateDeploymentSafety);
        assertEquals("prod mode requires gRPC TLS when matcher.grpcBindHost is not loopback", error.getMessage());
    }

    @Test
    void prodModeRejectsAeronPreviewWithoutExplicitOptIn() {
        Path dir = Path.of("target/prod-preview-reject");
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.PROD,
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
                8080,
                "127.0.0.1",
                4,
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
                9090,
                GrpcReplicationServerConfig.defaults(9090),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                MatcherClusterConfig.defaults(new TestLeaseStore(), new TestNodeRegistry(), "127.0.0.1", "merchant:42")
                        .withReplicationTransport(
                                ReplicationTransportType.AERON_PREVIEW,
                                new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 15090, 11001),
                                ReplicationTransportPolicyConfig.defaults())
        );
        IllegalStateException error = assertThrows(IllegalStateException.class, config::validateDeploymentSafety);
        assertEquals("prod mode forbids matcher.replicationTransport=AERON_PREVIEW unless matcher.allowPreviewTransportInProd=true",
                error.getMessage());
    }

    @Test
    void prodModeAllowsRemoteGrpcBindWhenAuthoritativeTransportIsAeron() {
        Path dir = Path.of("target/prod-aeron-grpc-optional");
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.PROD,
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
                8080,
                "127.0.0.1",
                4,
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
                9090,
                GrpcReplicationServerConfig.defaults(9090).withBindHost("10.0.0.10"),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                MatcherClusterConfig.defaults(new TestLeaseStore(), new TestNodeRegistry(), "127.0.0.1", "merchant:42")
                        .withReplicationTransport(
                                ReplicationTransportType.AERON,
                                new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 15090, 11001),
                                ReplicationTransportPolicyConfig.defaults()
                        )
        );
        config.validateDeploymentSafety();
        assertEquals(false, config.requiresGrpcReplicationServer());
    }

    @Test
    void prodModeRejectsRemoteAeronWithoutTransportSecurity() {
        Path dir = Path.of("target/prod-aeron-security-required");
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.PROD,
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
                8080,
                "127.0.0.1",
                4,
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
                9090,
                GrpcReplicationServerConfig.defaults(9090).withBindHost("10.0.0.10"),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                MatcherClusterConfig.defaults(new TestLeaseStore(), new TestNodeRegistry(), "10.0.0.10", "merchant:42")
                        .withReplicationTransport(
                                ReplicationTransportType.AERON,
                                new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 15090, 11001),
                                ReplicationTransportPolicyConfig.defaults()
                        )
        );
        IllegalStateException error = assertThrows(IllegalStateException.class, config::validateDeploymentSafety);
        assertEquals(
                "prod mode requires transport security when matcher.replicationTransport=AERON and matcher.advertisedHost is not loopback",
                error.getMessage()
        );
    }

    @Test
    void transportChangeRequiresExplicitWindow() throws Exception {
        Path dir = Files.createTempDirectory("transport-policy-lock");
        MatcherClusterConfig clusterConfig = MatcherClusterConfig.defaults(
                        new TestLeaseStore(),
                        new TestNodeRegistry(),
                        "127.0.0.1",
                        "merchant:42")
                .withReplicationTransport(
                        ReplicationTransportType.GRPC,
                        new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 15090, 11001),
                        ReplicationTransportPolicyConfig.defaults());
        MatcherServerConfig grpcConfig = baseConfig(dir, clusterConfig);
        ReplicationTransportPolicyEnforcer.validateAndLock(grpcConfig);
        MatcherClusterConfig switchedConfig = clusterConfig.withReplicationTransport(
                ReplicationTransportType.AERON_PREVIEW,
                clusterConfig.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults());
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> ReplicationTransportPolicyEnforcer.validateAndLock(baseConfig(dir, switchedConfig)));
        assertEquals(
                "replication transport change from GRPC to AERON_PREVIEW requires matcher.allowTransportChange=true and matcher.transportChangeWindowId",
                error.getMessage());
    }

    private static MatcherServerConfig baseConfig(Path dir, MatcherClusterConfig clusterConfig) {
        return new MatcherServerConfig(
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
                8080,
                "127.0.0.1",
                4,
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
                9090,
                GrpcReplicationServerConfig.defaults(9090),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                clusterConfig
        );
    }

    private static final class TestLeaseStore implements LeaseStore {
        @Override
        public ClusterLease currentLease() {
            return new ClusterLease("node-a", new FencingToken(1L), System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            return true;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            return true;
        }
    }

    private static final class TestNodeRegistry implements NodeRegistry {
        @Override
        public void registerOrUpdate(DiscoveredNode node) {
        }

        @Override
        public void unregister(String nodeId) {
        }

        @Override
        public List<DiscoveredNode> listNodes() {
            return List.of();
        }
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
