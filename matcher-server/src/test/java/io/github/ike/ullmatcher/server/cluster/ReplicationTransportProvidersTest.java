package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportProvider;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ReplicationTransportProvidersTest {
    @Test
    void creationIsDrivenByTheConfiguredReplicationTransportType() throws Exception {
        Path dir = Files.createTempDirectory("transport-provider-factory");
        MatcherServerConfig config = MatcherServerConfig.builder("node-a", 1, dir)
                .ringCapacity(1 << 10)
                .gatewaySpinLimit(128)
                .gatewayOfferTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(200))
                .httpPort(0)
                .httpWorkerThreads(2)
                .build();
        MatcherClusterConfig grpcCluster = MatcherClusterConfig.defaults(
                new StubLeaseStore(), new StubNodeRegistry(), "127.0.0.1", "symbol-1");
        ServerSecurityConfig securityConfig = ServerSecurityConfig.insecureDefaults();

        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();

            try (ReplicationTransportProvider provider =
                         ReplicationTransportProviders.create(grpcCluster, securityConfig, nodeService)) {
                assertEquals(ReplicationTransportType.GRPC, provider.type());
            }

            MatcherClusterConfig aeronCluster = grpcCluster.withReplicationTransport(
                    ReplicationTransportType.AERON,
                    new AeronPreviewTransportConfig(dir.resolve("aeron-authoritative"), 21_110, 17_110),
                    ReplicationTransportPolicyConfig.defaults());
            try (ReplicationTransportProvider provider =
                         ReplicationTransportProviders.create(aeronCluster, securityConfig, nodeService)) {
                assertEquals(ReplicationTransportType.AERON, provider.type());
            }

            MatcherClusterConfig previewCluster = grpcCluster.withReplicationTransport(
                    ReplicationTransportType.AERON_PREVIEW,
                    new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 21_120, 17_120),
                    ReplicationTransportPolicyConfig.defaults());
            try (ReplicationTransportProvider provider =
                         ReplicationTransportProviders.create(previewCluster, securityConfig, nodeService)) {
                assertEquals(ReplicationTransportType.AERON_PREVIEW, provider.type());
            }

            assertEquals("clusterConfig",
                    assertThrows(NullPointerException.class,
                            () -> ReplicationTransportProviders.create(null, securityConfig, nodeService)).getMessage());
            assertEquals("securityConfig",
                    assertThrows(NullPointerException.class,
                            () -> ReplicationTransportProviders.create(grpcCluster, null, nodeService)).getMessage());
            assertEquals("nodeService",
                    assertThrows(NullPointerException.class,
                            () -> ReplicationTransportProviders.create(grpcCluster, securityConfig, null)).getMessage());
        }
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
