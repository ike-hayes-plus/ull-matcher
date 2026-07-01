package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.ReplicationTransportProvider;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.ha.transport.ClusterPeerClient;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.grpc.client.GrpcReplicationClientConfig;
import io.github.ike.ullmatcher.ha.grpc.client.GrpcReplicationTarget;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

final class GrpcReplicationTransportProvider implements ReplicationTransportProvider {
    private final ServerSecurityConfig securityConfig;

    GrpcReplicationTransportProvider(ServerSecurityConfig securityConfig) {
        this.securityConfig = Objects.requireNonNull(securityConfig, "securityConfig");
    }

    @Override
    public ReplicationTransportType type() {
        return ReplicationTransportType.GRPC;
    }

    @Override
    public Map<String, String> localNodeMetadata() {
        return Map.of(TRANSPORT_METADATA_KEY, type().name());
    }

    @Override
    public ClusterPeerClient connect(DiscoveredNode node) throws IOException {
        return new GrpcClusterPeerClient(GrpcReplicationTarget.connect(node.nodeId(), clientConfig(node)));
    }

    @Override
    public TransportMetricsSnapshot metricsSnapshot() {
        return TransportMetricsSnapshot.none(type().name());
    }

    @Override
    public void close() {
    }

    private GrpcReplicationClientConfig clientConfig(DiscoveredNode node) {
        return new GrpcReplicationClientConfig(
                node.host(),
                node.grpcPort(),
                4 << 20,
                "identity",
                256,
                512 << 10,
                securityConfig.grpcClientTls()
        );
    }
}
