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
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
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
                () -> MatcherServerMain.nodeRegistry("unsupported", "127.0.0.1:2181", "http://127.0.0.1:2379", "cluster-a"));
        assertEquals("unsupported discovery provider: unsupported", error.getMessage());
    }

    @Test
    void unsupportedLeaseProviderFailsFast() {
        ServerBootstrapException error = assertThrows(ServerBootstrapException.class,
                () -> MatcherServerMain.leaseStore("unsupported", "127.0.0.1:2181", "http://127.0.0.1:2379", "cluster-a"));
        assertEquals("unsupported lease provider: unsupported", error.getMessage());
    }

    @Test
    void etcdProviderRequiresEndpoint() {
        ServerBootstrapException error = assertThrows(ServerBootstrapException.class,
                () -> MatcherServerMain.etcdConfig("", "cluster-a"));
        assertEquals("matcher.etcdEndpoint is required when using etcd provider", error.getMessage());
    }

    @Test
    void etcdProviderUsesConfiguredOverrides() {
        String previousPrefix = System.getProperty("matcher.etcdKeyPrefix");
        String previousTtl = System.getProperty("matcher.etcdLeaseTtlSeconds");
        String previousTimeout = System.getProperty("matcher.etcdTimeoutMillis");
        String previousCache = System.getProperty("matcher.etcdLocalHeldCheckCacheMillis");
        try {
            System.setProperty("matcher.etcdKeyPrefix", "/matcher/prod");
            System.setProperty("matcher.etcdLeaseTtlSeconds", "15");
            System.setProperty("matcher.etcdTimeoutMillis", "750");
            System.setProperty("matcher.etcdLocalHeldCheckCacheMillis", "10");
            var config = MatcherServerMain.etcdConfig("http://127.0.0.1:2379", "cluster-a");
            assertEquals("http://127.0.0.1:2379", config.endpoint());
            assertEquals("/matcher/prod", config.keyPrefix());
            assertEquals(15L, config.leaseTtlSeconds());
            assertEquals(750L, config.timeoutMillis());
            assertEquals(10L, config.localHeldCheckCacheMillis());
        } finally {
            restoreProperty("matcher.etcdKeyPrefix", previousPrefix);
            restoreProperty("matcher.etcdLeaseTtlSeconds", previousTtl);
            restoreProperty("matcher.etcdTimeoutMillis", previousTimeout);
            restoreProperty("matcher.etcdLocalHeldCheckCacheMillis", previousCache);
        }
    }

    @Test
    void etcdEndpointDefaultsLeaseAndDiscoveryProviders() throws Exception {
        String previousZk = System.getProperty("matcher.zkConnect");
        String previousEtcd = System.getProperty("matcher.etcdEndpoint");
        String previousLease = System.getProperty("matcher.leaseProvider");
        String previousDiscovery = System.getProperty("matcher.discoveryProvider");
        try {
            restoreProperty("matcher.zkConnect", null);
            System.setProperty("matcher.etcdEndpoint", "http://127.0.0.1:2379");
            restoreProperty("matcher.leaseProvider", null);
            restoreProperty("matcher.discoveryProvider", null);

            MatcherClusterConfig clusterConfig = MatcherServerMain.clusterConfig("merchant:42");

            assertEquals("EtcdLeaseStore", clusterConfig.leaseStore().getClass().getSimpleName());
            assertEquals("EtcdNodeRegistry", clusterConfig.nodeRegistry().getClass().getSimpleName());
        } finally {
            restoreProperty("matcher.zkConnect", previousZk);
            restoreProperty("matcher.etcdEndpoint", previousEtcd);
            restoreProperty("matcher.leaseProvider", previousLease);
            restoreProperty("matcher.discoveryProvider", previousDiscovery);
        }
    }

    @Test
    void standaloneClusterConfigRejectsMixedControlPlaneProviders() {
        String previousZk = System.getProperty("matcher.zkConnect");
        String previousEtcd = System.getProperty("matcher.etcdEndpoint");
        String previousLease = System.getProperty("matcher.leaseProvider");
        String previousDiscovery = System.getProperty("matcher.discoveryProvider");
        try {
            System.setProperty("matcher.zkConnect", "127.0.0.1:2181");
            System.setProperty("matcher.etcdEndpoint", "http://127.0.0.1:2379");
            System.setProperty("matcher.leaseProvider", "zk");
            System.setProperty("matcher.discoveryProvider", "etcd");

            ServerBootstrapException error = assertThrows(ServerBootstrapException.class,
                    () -> MatcherServerMain.clusterConfig("merchant:42"));

            assertEquals("matcher.leaseProvider and matcher.discoveryProvider must match: zk != etcd",
                    error.getMessage());
        } finally {
            restoreProperty("matcher.zkConnect", previousZk);
            restoreProperty("matcher.etcdEndpoint", previousEtcd);
            restoreProperty("matcher.leaseProvider", previousLease);
            restoreProperty("matcher.discoveryProvider", previousDiscovery);
        }
    }

    @Test
    void zkDiscoveryProviderRequiresConnectString() {
        ServerBootstrapException error = assertThrows(ServerBootstrapException.class,
                () -> MatcherServerMain.nodeRegistry("zk", "", "http://127.0.0.1:2379", "cluster-a"));
        assertEquals("matcher.zkConnect is required when matcher.discoveryProvider=zk", error.getMessage());
    }

    @Test
    void standaloneClusterConfigBindsFailoverAndReplicationModeProperties() throws Exception {
        String previousZk = System.getProperty("matcher.zkConnect");
        String previousHeartbeat = System.getProperty("matcher.failoverPrimaryHeartbeatTimeoutMillis");
        String previousLag = System.getProperty("matcher.failoverMaxPromotionLag");
        String previousMinStandbys = System.getProperty("matcher.failoverMinStandbyReplicas");
        String previousReplicationMode = System.getProperty("matcher.replicationMode");
        try {
            System.setProperty("matcher.zkConnect", "127.0.0.1:2181");
            System.setProperty("matcher.failoverPrimaryHeartbeatTimeoutMillis", "2500");
            System.setProperty("matcher.failoverMaxPromotionLag", "7");
            System.setProperty("matcher.failoverMinStandbyReplicas", "2");
            System.setProperty("matcher.replicationMode", "WAIT_FOR_QUORUM_STANDBYS");

            MatcherClusterConfig clusterConfig = MatcherServerMain.clusterConfig("merchant:42");

            assertEquals(TimeUnit.MILLISECONDS.toNanos(2_500L), clusterConfig.failoverPolicy().primaryHeartbeatTimeoutNanos());
            assertEquals(7L, clusterConfig.failoverPolicy().maxPromotionLag());
            assertEquals(2, clusterConfig.failoverPolicy().minStandbyReplicas());
            assertEquals(io.github.ike.ullmatcher.ha.replication.ReplicationMode.WAIT_FOR_QUORUM_STANDBYS,
                    clusterConfig.replicationMode());
        } finally {
            restoreProperty("matcher.zkConnect", previousZk);
            restoreProperty("matcher.failoverPrimaryHeartbeatTimeoutMillis", previousHeartbeat);
            restoreProperty("matcher.failoverMaxPromotionLag", previousLag);
            restoreProperty("matcher.failoverMinStandbyReplicas", previousMinStandbys);
            restoreProperty("matcher.replicationMode", previousReplicationMode);
        }
    }

    @Test
    void configRejectsNonPowerOfTwoRingCapacity() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> baseConfigWithSizing(Path.of("target/invalid-ring"), 1_000, 256));
        assertEquals("ringCapacity must be a power of two", error.getMessage());
    }

    @Test
    void configRejectsBinaryBatchLargerThanRingCapacity() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> baseConfigWithSizing(Path.of("target/invalid-binary-batch"), 128, 256));
        assertEquals("binaryIngressMaxBatchSize must not exceed ringCapacity", error.getMessage());
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

    private static MatcherServerConfig baseConfigWithSizing(Path dir, int ringCapacity, int binaryIngressMaxBatchSize) {
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
                ringCapacity,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                8080,
                "127.0.0.1",
                4,
                1 << 20,
                256,
                2_000L,
                false,
                10080,
                "127.0.0.1",
                binaryIngressMaxBatchSize,
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
