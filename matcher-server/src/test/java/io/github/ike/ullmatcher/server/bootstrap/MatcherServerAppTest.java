package io.github.ike.ullmatcher.server.bootstrap;

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
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportProvider;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.server.cluster.AeronTransportConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.storage.wal.WalArchiveConfig;
import io.github.ike.ullmatcher.server.telemetry.ReadinessSnapshot;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherServerAppTest {
    @Test
    void standaloneAppStartsEveryIngressOnEphemeralPortsAndStopsCleanly() throws Exception {
        Path dir = Files.createTempDirectory("matcher-app-standalone");
        MatcherServerConfig config = baseBuilder(dir)
                .binaryIngressEnabled(true)
                .securityConfig(ServerSecurityConfig.fromPaths(null, null, null, false, 0L, true))
                .build();

        try (MatcherServerApp app = new MatcherServerApp(config)) {
            app.start();

            assertTrue(app.httpPort() > 0);
            assertTrue(app.grpcPort() > 0);
            assertTrue(app.binaryIngressPort() > 0);
            assertNotNull(app.nodeService());
            assertTrue(await(() -> app.nodeService().health().acceptingClientCommands(), 5_000L));

            HttpClient client = HttpClient.newHttpClient();
            assertEquals(200, statusOf(client, app.httpPort(), "/api/v1/runtime/live"));
            assertEquals(200, statusOf(client, app.httpPort(), "/api/v1/runtime/health"));
            assertEquals(200, statusOf(client, app.httpPort(), "/api/v1/runtime/health"));
            assertEquals(200, statusOf(client, app.httpPort(), "/api/v1/runtime/readiness"));
            assertEquals(200, statusOf(client, app.httpPort(), "/metrics"));

            ReadinessSnapshot readiness = app.readinessSnapshot();
            assertTrue(readiness.serviceReady());
            assertTrue(readiness.clientTrafficReady());
            assertTrue(readiness.promotionReady());
            assertFalse(readiness.snapshotSyncRequired());
            assertFalse(readiness.catchUpInProgress());
            assertFalse(readiness.tlsReloadInProgress());
            assertEquals("IDLE", readiness.syncState());
            assertEquals("NONE", readiness.lastTickAction());
            assertEquals("ready", readiness.reason());
        }
    }

    @Test
    void appWithoutBinaryIngressOrGrpcReplicationReportsDisabledPorts() throws Exception {
        Path dir = Files.createTempDirectory("matcher-app-aeron-only");
        MatcherClusterConfig clusterConfig = clusterConfig(new StubLeaseStore("node-a"), new StubNodeRegistry())
                .withReplicationTransport(
                        ReplicationTransportType.AERON,
                        new AeronTransportConfig(dir.resolve("aeron"), 15_810, 11_810),
                        ReplicationTransportPolicyConfig.defaults());
        MatcherServerConfig config = baseBuilder(dir).clusterConfig(clusterConfig).build();

        try (MatcherServerApp app = new MatcherServerApp(config)) {
            app.start();

            assertEquals(-1, app.grpcPort());
            assertEquals(-1, app.binaryIngressPort());
            assertTrue(app.httpPort() > 0);
            assertTrue(await(() -> app.readinessSnapshot().lastTickResult() != null, 10_000L));

            ReadinessSnapshot readiness = app.readinessSnapshot();
            assertEquals("STABLE", readiness.transportPolicyStatus());
            assertEquals("DISABLED", readiness.transportReconciliationStatus());
            assertEquals(0L, readiness.transportSecurityGeneration());
        }
    }

    @Test
    void clusteredAppProbesTheDiscoveredPrimaryAndPublishesGateDecisions() throws Exception {
        Path dir = Files.createTempDirectory("matcher-app-clustered");
        StubNodeRegistry registry = new StubNodeRegistry();
        registry.registerOrUpdate(new DiscoveredNode(
                "node-b",
                "127.0.0.1",
                1,
                HaRole.PRIMARY,
                Map.of("shardKey", "symbol-1", ReplicationTransportProvider.TRANSPORT_METADATA_KEY, ReplicationTransportType.GRPC.name())));
        MatcherServerConfig config = baseBuilder(dir)
                .initialRole(HaRole.STANDBY)
                .clusterConfig(clusterConfig(new StubLeaseStore("node-b"), registry))
                .build();

        try (MatcherServerApp app = new MatcherServerApp(config)) {
            app.start();

            assertTrue(await(() -> app.readinessSnapshot().lastGateDecision() != null, 10_000L));
            assertTrue(await(() -> registry.listNodes().size() == 2, 5_000L));

            ReadinessSnapshot readiness = app.readinessSnapshot();
            assertNotNull(readiness.lastTickResult());
            assertFalse(readiness.lastTickResult().action().isBlank());
            assertFalse(readiness.lastTickResult().roleBefore().isBlank());
            assertFalse(readiness.lastTickResult().roleAfter().isBlank());
            assertNotNull(readiness.lastGateDecision());
            assertEquals("STABLE", readiness.transportPolicyStatus());
            assertFalse(readiness.lastTickAction().isBlank());
        }
    }

    @Test
    void transportDriftAcrossPeersIsSurfacedInReadiness() throws Exception {
        Path dir = Files.createTempDirectory("matcher-app-drift");
        StubNodeRegistry registry = new StubNodeRegistry();
        registry.registerOrUpdate(new DiscoveredNode(
                "node-b",
                "127.0.0.1",
                1,
                HaRole.STANDBY,
                Map.of("shardKey", "symbol-1", ReplicationTransportProvider.TRANSPORT_METADATA_KEY, ReplicationTransportType.AERON.name())));
        MatcherServerConfig config = baseBuilder(dir)
                .clusterConfig(clusterConfig(new StubLeaseStore("node-a"), registry))
                .build();

        try (MatcherServerApp app = new MatcherServerApp(config)) {
            app.start();

            assertTrue(await(() -> "DRIFT".equals(app.readinessSnapshot().transportPolicyStatus()), 10_000L));

            ReadinessSnapshot readiness = app.readinessSnapshot();
            assertFalse(readiness.serviceReady());
            assertEquals("TRANSPORT_DRIFT", readiness.syncState());
            assertTrue(readiness.reason().startsWith("replication transport drift detected for shard symbol-1"),
                    readiness.reason());
            assertTrue(readiness.transportPolicyConclusion().contains("node-b[AERON]"),
                    readiness.transportPolicyConclusion());
        }
    }

    @Test
    void appRefusesToStartWhenDeploymentSafetyFails() throws Exception {
        Path dir = Files.createTempDirectory("matcher-app-unsafe");
        MatcherServerConfig config = baseBuilder(dir)
                .serverMode(MatcherServerMode.PROD)
                .persistenceProfile(PersistenceProfile.PROD)
                .snapshotIntervalMillis(PersistenceProfile.PROD_SNAPSHOT_INTERVAL_MILLIS)
                .walArchiveConfig(WalArchiveConfig.ofDirectory(dir.resolve("wal-cold")))
                .httpBindHost("10.0.0.10")
                .build();

        assertEquals("prod mode requires matcher.ingressApiKeys when HTTP/binary bind to non-loopback addresses",
                assertThrows(IllegalStateException.class, () -> new MatcherServerApp(config)).getMessage());
    }

    @Test
    void appRefusesAnUnannouncedReplicationTransportChange() throws Exception {
        Path dir = Files.createTempDirectory("matcher-app-transport-change");
        StubLeaseStore leaseStore = new StubLeaseStore("node-a");
        StubNodeRegistry registry = new StubNodeRegistry();
        MatcherServerConfig grpcConfig = baseBuilder(dir)
                .clusterConfig(clusterConfig(leaseStore, registry))
                .build();
        try (MatcherServerApp app = new MatcherServerApp(grpcConfig)) {
            app.start();
        }

        MatcherServerConfig switchedConfig = baseBuilder(dir)
                .clusterConfig(clusterConfig(leaseStore, registry).withReplicationTransport(
                        ReplicationTransportType.AERON,
                        new AeronTransportConfig(dir.resolve("aeron"), 15_820, 11_820),
                        ReplicationTransportPolicyConfig.defaults()))
                .build();

        assertEquals("replication transport change from GRPC to AERON requires "
                        + "matcher.allowTransportChange=true and matcher.transportChangeWindowId",
                assertThrows(IllegalStateException.class, () -> new MatcherServerApp(switchedConfig)).getMessage());
    }

    private static MatcherServerConfig.Builder baseBuilder(Path dir) {
        return MatcherServerConfig.builder("node-a", 1, dir)
                .ringCapacity(1 << 10)
                .gatewaySpinLimit(128)
                .gatewayOfferTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(200))
                .httpPort(0)
                .httpWorkerThreads(2)
                .httpMaxBodyBytes(4_096)
                .binaryIngressPort(0)
                .binaryIngressMaxBatchSize(128)
                // The advertised port is what peers dial; the server itself binds an ephemeral port.
                .grpcPort(19_991)
                .grpcServerConfig(GrpcReplicationServerConfig.defaults(0))
                .ingressAuthConfig(IngressAuthConfig.disabled());
    }

    private static MatcherClusterConfig clusterConfig(LeaseStore leaseStore, NodeRegistry nodeRegistry) {
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
                new AeronTransportConfig(Path.of("target", "matcher-aeron"), 15_900, 11_900),
                ReplicationTransportPolicyConfig.defaults()
        );
    }

    private static int statusOf(HttpClient client, int port, String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString()
        ).statusCode();
    }

    private static boolean await(Check check, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (check.ok()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return check.ok();
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    private static final class StubNodeRegistry implements NodeRegistry {
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

    private static final class StubLeaseStore implements LeaseStore {
        private volatile ClusterLease lease;

        private StubLeaseStore(String ownerNodeId) {
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
