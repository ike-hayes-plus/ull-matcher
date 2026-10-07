package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.runtime.MatchLoopConfig;
import io.github.ike.ullmatcher.server.api.BinaryIngressLimits;
import io.github.ike.ullmatcher.server.cluster.AeronTransportConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.storage.wal.WalArchiveConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherServerConfigBuilderTest {
    private static MatcherServerConfig.Builder prodBuilder(Path dir) {
        return MatcherServerConfig.builder("node-a", 1, dir)
                .serverMode(MatcherServerMode.PROD)
                .persistenceProfile(PersistenceProfile.PROD)
                .snapshotIntervalMillis(PersistenceProfile.PROD_SNAPSHOT_INTERVAL_MILLIS)
                .walArchiveConfig(WalArchiveConfig.ofDirectory(dir.resolve("wal-cold")));
    }

    @Test
    void builderAppliesEveryComponentOverride() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-overrides");
        MatcherConfig matcherConfig = MatcherConfig.defaults(7);
        BinaryIngressLimits binaryLimits = new BinaryIngressLimits(64, 1_500L, 30_000L);
        WriteAdmissionPolicyConfig admissionPolicy =
                new WriteAdmissionPolicyConfig(11, 7, "X-Tenant", 1.5d, 3, 2.5d, 4, 2, "a=3", "X-Priority");
        IngressAuthConfig ingressAuth = IngressAuthConfig.fromCommaSeparated("k1", "X-Key");
        GrpcReplicationServerConfig grpcServerConfig = GrpcReplicationServerConfig.defaults(19_091);
        ServerSecurityConfig securityConfig = ServerSecurityConfig.insecureDefaults();
        TtlCancelConfig ttlCancelConfig = TtlCancelConfig.defaults();
        MatchLoopConfig loopConfig = MatchLoopConfig.defaults();
        StandbySyncConfig standbySyncConfig = StandbySyncConfig.defaults();
        MatcherClusterConfig clusterConfig = MatcherClusterConfig.defaults(
                new StubLeaseStore(), new StubNodeRegistry(), "127.0.0.1", "merchant:7");

        MatcherServerConfig config = MatcherServerConfig.builder("seed", 1, dir)
                .serverMode(MatcherServerMode.DEV)
                .nodeId("node-z")
                .shardKey("merchant:7")
                .matcherConfig(matcherConfig)
                .walDirectory(dir.resolve("custom-wal"))
                .walPrefix("custom-prefix")
                .walSegmentSizeBytes(8L * 1024L * 1024L)
                .walDurabilityMode(WalDurabilityMode.OS_BUFFERED)
                .walForceBatchSize(4)
                .walForceMaxDelayMicros(250L)
                .snapshotFile(dir.resolve("custom.snap"))
                .ringCapacity(1 << 9)
                .gatewaySpinLimit(64)
                .gatewayOfferTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(150))
                .httpPort(18_080)
                .httpBindHost("127.0.0.1")
                .httpWorkerThreads(3)
                .httpMaxBodyBytes(4_096)
                .httpMaxConcurrentRequests(32)
                .httpRequestTimeoutMillis(1_500L)
                .binaryIngressEnabled(true)
                .binaryIngressPort(18_081)
                .binaryIngressBindHost("127.0.0.1")
                .binaryIngressMaxBatchSize(128)
                .binaryIngressLimits(binaryLimits)
                .httpWriteMaxConcurrentRequests(21)
                .httpReadMaxConcurrentRequests(22)
                .httpAdminMaxConcurrentRequests(23)
                .httpWriteTimeoutMillis(2_100L)
                .httpReadTimeoutMillis(2_200L)
                .httpAdminTimeoutMillis(2_300L)
                .httpSubmitEndpointMaxConcurrentRequests(31)
                .httpCancelEndpointMaxConcurrentRequests(32)
                .httpSnapshotEndpointMaxConcurrentRequests(33)
                .httpReadinessEndpointMaxConcurrentRequests(34)
                .httpMetricsEndpointMaxConcurrentRequests(35)
                .writeAdmissionPolicyConfig(admissionPolicy)
                .allowInsecureRemoteHttp(true)
                .ingressAuthConfig(ingressAuth)
                .grpcPort(19_091)
                .grpcServerConfig(grpcServerConfig)
                .securityConfig(securityConfig)
                .ttlCancelConfig(ttlCancelConfig)
                .initialRole(HaRole.STANDBY)
                .loopConfig(loopConfig)
                .standbySyncConfig(standbySyncConfig)
                .clusterConfig(clusterConfig)
                .build();

        assertEquals(MatcherServerMode.DEV, config.serverMode());
        assertEquals("node-z", config.nodeId());
        assertEquals("merchant:7", config.shardKey());
        assertSame(matcherConfig, config.matcherConfig());
        assertEquals(dir.resolve("custom-wal"), config.walDirectory());
        assertEquals("custom-prefix", config.walPrefix());
        assertEquals(8L * 1024L * 1024L, config.walSegmentSizeBytes());
        assertEquals(WalDurabilityMode.OS_BUFFERED, config.walDurabilityMode());
        assertEquals(4, config.walForceBatchSize());
        assertEquals(250L, config.walForceMaxDelayMicros());
        assertEquals(dir.resolve("custom.snap"), config.snapshotFile());
        assertEquals(1 << 9, config.ringCapacity());
        assertEquals(64, config.gatewaySpinLimit());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(150), config.gatewayOfferTimeoutNanos());
        assertEquals(18_080, config.httpPort());
        assertEquals("127.0.0.1", config.httpBindHost());
        assertEquals(3, config.httpWorkerThreads());
        assertEquals(4_096, config.httpMaxBodyBytes());
        assertEquals(32, config.httpMaxConcurrentRequests());
        assertEquals(1_500L, config.httpRequestTimeoutMillis());
        assertTrue(config.binaryIngressEnabled());
        assertEquals(18_081, config.binaryIngressPort());
        assertEquals("127.0.0.1", config.binaryIngressBindHost());
        assertEquals(128, config.binaryIngressMaxBatchSize());
        assertSame(binaryLimits, config.binaryIngressLimits());
        assertEquals(21, config.httpWriteMaxConcurrentRequests());
        assertEquals(22, config.httpReadMaxConcurrentRequests());
        assertEquals(23, config.httpAdminMaxConcurrentRequests());
        assertEquals(2_100L, config.httpWriteTimeoutMillis());
        assertEquals(2_200L, config.httpReadTimeoutMillis());
        assertEquals(2_300L, config.httpAdminTimeoutMillis());
        assertEquals(31, config.httpSubmitEndpointMaxConcurrentRequests());
        assertEquals(32, config.httpCancelEndpointMaxConcurrentRequests());
        assertEquals(33, config.httpSnapshotEndpointMaxConcurrentRequests());
        assertEquals(34, config.httpReadinessEndpointMaxConcurrentRequests());
        assertEquals(35, config.httpMetricsEndpointMaxConcurrentRequests());
        assertSame(admissionPolicy, config.writeAdmissionPolicyConfig());
        assertTrue(config.allowInsecureRemoteHttp());
        assertSame(ingressAuth, config.ingressAuthConfig());
        assertEquals(19_091, config.grpcPort());
        assertSame(grpcServerConfig, config.grpcServerConfig());
        assertSame(securityConfig, config.securityConfig());
        assertSame(ttlCancelConfig, config.ttlCancelConfig());
        assertEquals(HaRole.STANDBY, config.initialRole());
        assertSame(loopConfig, config.loopConfig());
        assertSame(standbySyncConfig, config.standbySyncConfig());
        assertSame(clusterConfig, config.clusterConfig());

        assertEquals(11, config.httpShardWriteMaxConcurrentRequests());
        assertEquals(7, config.httpTenantWriteMaxConcurrentRequests());
        assertEquals("X-Tenant", config.httpTenantAdmissionHeader());
    }

    @Test
    void toBuilderRoundTripsTheConfigUnchanged() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-round-trip");
        MatcherServerConfig config = MatcherServerConfig.defaults("node-a", 3, dir);

        assertEquals(config, config.toBuilder().build());
    }

    @Test
    void withClusterConfigReplacesOnlyTheClusterComponent() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-cluster");
        MatcherServerConfig standalone = MatcherServerConfig.defaults("node-a", 1, dir);
        MatcherClusterConfig clusterConfig = MatcherClusterConfig.defaults(
                new StubLeaseStore(), new StubNodeRegistry(), "127.0.0.1", standalone.shardKey());

        MatcherServerConfig clustered = standalone.withClusterConfig(clusterConfig);

        assertSame(clusterConfig, clustered.clusterConfig());
        assertEquals(standalone, clustered.withClusterConfig(null));
    }

    @Test
    void requiresGrpcReplicationServerFollowsTheConfiguredTransport() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-transport");
        MatcherServerConfig standalone = MatcherServerConfig.defaults("node-a", 1, dir);
        MatcherClusterConfig grpcCluster = MatcherClusterConfig.defaults(
                new StubLeaseStore(), new StubNodeRegistry(), "127.0.0.1", standalone.shardKey());

        assertTrue(standalone.requiresGrpcReplicationServer());
        assertTrue(standalone.withClusterConfig(grpcCluster).requiresGrpcReplicationServer());
        assertFalse(standalone.withClusterConfig(grpcCluster.withReplicationTransport(
                        ReplicationTransportType.AERON,
                        grpcCluster.aeronTransportConfig(),
                        ReplicationTransportPolicyConfig.defaults()))
                .requiresGrpcReplicationServer());
    }

    @Test
    void nullIngressAuthAndBinaryLimitsFallBackToSafeDefaults() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-null-components");

        MatcherServerConfig config = MatcherServerConfig.builder("node-a", 1, dir)
                .ingressAuthConfig(null)
                .binaryIngressLimits(null)
                .build();

        assertFalse(config.ingressAuthConfig().enabled());
        assertEquals(BinaryIngressLimits.defaults(), config.binaryIngressLimits());
    }

    @Test
    void blankIdentifiersAndInvalidSizingAreRejected() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-invalid");

        assertEquals("nodeId, shardKey, walPrefix, httpBindHost and binaryIngressBindHost must not be blank",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node-a", 1, dir).nodeId(" ").build()).getMessage());
        assertEquals("nodeId must not contain '|'",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node|a", 1, dir).build()).getMessage());
        assertEquals("nodeId, shardKey, walPrefix, httpBindHost and binaryIngressBindHost must not be blank",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node-a", 1, dir).binaryIngressBindHost("").build()).getMessage());
        assertEquals("invalid server sizing or port configuration",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node-a", 1, dir).httpWorkerThreads(0).build()).getMessage());
        assertEquals("invalid server sizing or port configuration",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node-a", 1, dir).grpcPort(-1).build()).getMessage());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).walDurabilityMode(null).build());
        assertEquals("nodeId, shardKey, walPrefix, httpBindHost and binaryIngressBindHost must not be blank",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node-a", 1, dir).shardKey("").build()).getMessage());
        assertEquals("nodeId, shardKey, walPrefix, httpBindHost and binaryIngressBindHost must not be blank",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node-a", 1, dir).walPrefix(" ").build()).getMessage());
        assertEquals("nodeId, shardKey, walPrefix, httpBindHost and binaryIngressBindHost must not be blank",
                assertThrows(IllegalArgumentException.class,
                        () -> MatcherServerConfig.builder("node-a", 1, dir).httpBindHost(null).build()).getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).walSegmentSizeBytes(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).walForceBatchSize(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).walForceMaxDelayMicros(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).ringCapacity(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).ringCapacity(3).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).gatewaySpinLimit(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).gatewayOfferTimeoutNanos(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpPort(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).binaryIngressPort(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpMaxBodyBytes(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).binaryIngressMaxBatchSize(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).binaryIngressMaxBatchSize(1 << 20).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpRequestTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpWriteMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpReadMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpAdminMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpWriteTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpReadTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpAdminTimeoutMillis(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpSubmitEndpointMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpCancelEndpointMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpSnapshotEndpointMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpReadinessEndpointMaxConcurrentRequests(0).build());
        assertThrows(IllegalArgumentException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).httpMetricsEndpointMaxConcurrentRequests(0).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).serverMode(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).matcherConfig(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).initialRole(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).ttlCancelConfig(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).writeAdmissionPolicyConfig(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).snapshotFile(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).grpcServerConfig(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).securityConfig(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).loopConfig(null).build());
        assertThrows(NullPointerException.class,
                () -> MatcherServerConfig.builder("node-a", 1, dir).standbySyncConfig(null).build());
    }

    @Test
    void prodModeRejectsOsBufferedWalDurability() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-prod-durability");
        MatcherServerConfig config = prodBuilder(dir)
                .walDurabilityMode(WalDurabilityMode.OS_BUFFERED)
                .build();

        assertEquals("prod mode forbids matcher.walDurabilityMode=OS_BUFFERED",
                assertThrows(IllegalStateException.class, config::validateDeploymentSafety).getMessage());
    }

    @Test
    void prodModeRequiresExplicitOptInForRemoteHttpAndBinaryIngress() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-prod-remote");
        MatcherServerConfig.Builder builder = prodBuilder(dir)
                .ingressAuthConfig(IngressAuthConfig.fromCommaSeparated("key", IngressAuthConfig.DEFAULT_API_KEY_HEADER));

        assertEquals("prod mode requires matcher.allowInsecureRemoteHttp=true when matcher.httpBindHost is not loopback",
                assertThrows(IllegalStateException.class,
                        () -> builder.httpBindHost("10.0.0.10").build().validateDeploymentSafety()).getMessage());
        assertEquals("prod mode requires matcher.allowInsecureRemoteHttp=true when matcher.binaryIngressBindHost is not loopback",
                assertThrows(IllegalStateException.class,
                        () -> builder.httpBindHost("localhost")
                                .binaryIngressEnabled(true)
                                .binaryIngressBindHost("10.0.0.10")
                                .build()
                                .validateDeploymentSafety()).getMessage());

        builder.binaryIngressBindHost("::1").build().validateDeploymentSafety();
    }

    @Test
    void prodModeRejectsEphemeralWalRemoteKeysMissingGrpcTlsAndInsecureAeron() throws Exception {
        Path ephemeral = Files.createTempDirectory("config-builder-prod-ephemeral").resolve("target").resolve("data");
        Files.createDirectories(ephemeral);
        MatcherServerConfig ephemeralConfig = prodBuilder(ephemeral).build();
        assertTrue(assertThrows(IllegalStateException.class, ephemeralConfig::validateDeploymentSafety)
                .getMessage().contains("persistent matcher.dataDir"));

        Path dir = Files.createTempDirectory("config-builder-prod-remote-keys");
        assertEquals("prod mode requires matcher.ingressApiKeys when HTTP/binary bind to non-loopback addresses",
                assertThrows(IllegalStateException.class, () -> prodBuilder(dir)
                        .httpBindHost("10.0.0.10")
                        .allowInsecureRemoteHttp(true)
                        .build()
                        .validateDeploymentSafety()).getMessage());

        assertEquals("prod mode requires gRPC mTLS when matcher.grpcBindHost is not loopback",
                assertThrows(IllegalStateException.class, () -> prodBuilder(dir)
                        .grpcServerConfig(GrpcReplicationServerConfig.defaults(9_090).withBindHost("10.0.0.10"))
                        .build()
                        .validateDeploymentSafety()).getMessage());

        Path cert = dir.resolve("tls.crt");
        Path key = dir.resolve("tls.key");
        Path ca = dir.resolve("ca.crt");
        assertEquals("prod mode requires gRPC mTLS when matcher.grpcBindHost is not loopback",
                assertThrows(IllegalStateException.class, () -> prodBuilder(dir)
                        .grpcServerConfig(GrpcReplicationServerConfig.defaults(9_090).withBindHost("10.0.0.10"))
                        .securityConfig(ServerSecurityConfig.fromPaths(cert, key, ca, false, 0L, false))
                        .build()
                        .validateDeploymentSafety()).getMessage());
        prodBuilder(dir)
                .grpcServerConfig(GrpcReplicationServerConfig.defaults(9_090).withBindHost("10.0.0.10"))
                .securityConfig(ServerSecurityConfig.fromPaths(cert, key, ca, true, 0L, false))
                .build()
                .validateDeploymentSafety();

        MatcherClusterConfig aeron = MatcherClusterConfig
                .defaults(new StubLeaseStore(), new StubNodeRegistry(), "10.0.0.10", "symbol-1")
                .withReplicationTransport(
                        ReplicationTransportType.AERON,
                        new AeronTransportConfig(dir.resolve("aeron"), 15_090, 11_001),
                        ReplicationTransportPolicyConfig.defaults());
        assertEquals("prod mode requires transport security when matcher.replicationTransport=AERON and matcher.advertisedHost is not loopback",
                assertThrows(IllegalStateException.class, () -> prodBuilder(dir)
                        .clusterConfig(aeron)
                        .build()
                        .validateDeploymentSafety()).getMessage());
    }

    @Test
    void defaultsExposeStandaloneDevSettings() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-defaults");

        MatcherServerConfig config = MatcherServerConfig.defaults("node-a", 5, dir);

        assertEquals(MatcherServerMode.DEV, config.serverMode());
        assertEquals("symbol-5", config.shardKey());
        assertEquals("symbol-5", config.walPrefix());
        assertEquals(dir.toAbsolutePath().normalize().resolve("wal"), config.walDirectory());
        assertEquals(MatcherServerConfig.DEFAULT_WAL_DURABILITY_MODE, config.walDurabilityMode());
        assertEquals(MatcherServerConfig.DEFAULT_WAL_FORCE_BATCH_SIZE, config.walForceBatchSize());
        assertEquals(MatcherServerConfig.DEFAULT_WAL_FORCE_MAX_DELAY_MICROS, config.walForceMaxDelayMicros());
        assertFalse(config.walArchiveConfig().enabled());
        assertFalse(config.binaryIngressEnabled());
        assertEquals(ReplicationMode.WAIT_FOR_ANY_STANDBY,
                MatcherClusterConfig.defaults(new StubLeaseStore(), new StubNodeRegistry(), "127.0.0.1", "symbol-5")
                        .replicationMode());
        config.validateDeploymentSafety();
    }

    @Test
    void prodModeRejectsInvalidWalColdArchiveDirectory() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-wal-cold");
        Path cold = dir.resolve("target").resolve("cold");
        Files.createDirectories(cold);
        MatcherServerConfig ephemeralCold = prodBuilder(dir)
                .walArchiveConfig(WalArchiveConfig.ofDirectory(cold))
                .build();
        assertTrue(assertThrows(IllegalStateException.class, ephemeralCold::validateDeploymentSafety)
                .getMessage().contains("matcher.walColdArchiveDir"));

        Path persistentCold = dir.resolve("cold");
        MatcherServerConfig sameAsWal = prodBuilder(dir)
                .walDirectory(dir.resolve("wal"))
                .walArchiveConfig(WalArchiveConfig.ofDirectory(dir.resolve("wal")))
                .build();
        assertEquals("matcher.walColdArchiveDir must not equal the hot WAL directory",
                assertThrows(IllegalStateException.class, sameAsWal::validateDeploymentSafety).getMessage());

        MatcherServerConfig valid = prodBuilder(dir)
                .walArchiveConfig(WalArchiveConfig.ofDirectory(persistentCold))
                .build();
        valid.validateDeploymentSafety();
    }

    @Test
    void prodModeRequiresColdArchiveAndPeriodicSnapshot() throws Exception {
        Path dir = Files.createTempDirectory("config-builder-prod-persistence");
        MatcherServerConfig missingCold = MatcherServerConfig.builder("node-a", 1, dir)
                .serverMode(MatcherServerMode.PROD)
                .persistenceProfile(PersistenceProfile.PROD)
                .snapshotIntervalMillis(PersistenceProfile.PROD_SNAPSHOT_INTERVAL_MILLIS)
                .build();
        assertTrue(assertThrows(IllegalStateException.class, missingCold::validateDeploymentSafety)
                .getMessage().contains("matcher.walColdArchiveDir"));

        MatcherServerConfig missingSnapshot = prodBuilder(dir)
                .snapshotIntervalMillis(0L)
                .build();
        assertTrue(assertThrows(IllegalStateException.class, missingSnapshot::validateDeploymentSafety)
                .getMessage().contains("matcher.snapshotIntervalMillis"));

        MatcherServerConfig labInProd = prodBuilder(dir)
                .persistenceProfile(PersistenceProfile.LAB)
                .build();
        assertEquals("prod mode forbids matcher.persistenceProfile=LAB",
                assertThrows(IllegalStateException.class, labInProd::validateDeploymentSafety).getMessage());

        MatcherServerConfig benchInProd = prodBuilder(dir)
                .persistenceProfile(PersistenceProfile.BENCH)
                .build();
        assertEquals("prod mode forbids matcher.persistenceProfile=BENCH",
                assertThrows(IllegalStateException.class, benchInProd::validateDeploymentSafety).getMessage());

        MatcherServerConfig batchedWalInProd = prodBuilder(dir)
                .walDurabilityMode(WalDurabilityMode.SYNC_PER_BATCH)
                .build();
        assertEquals("prod mode forbids matcher.walDurabilityMode=SYNC_PER_BATCH",
                assertThrows(IllegalStateException.class, batchedWalInProd::validateDeploymentSafety).getMessage());
    }

    private static final class StubLeaseStore implements LeaseStore {
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

    private static final class StubNodeRegistry implements NodeRegistry {
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
}
