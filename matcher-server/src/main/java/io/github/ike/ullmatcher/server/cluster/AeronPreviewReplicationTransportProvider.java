package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.ReplicationTransportProvider;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.ha.transport.ClusterPeerClient;
import io.aeron.Aeron;
import io.github.ike.ullmatcher.ha.aeron.AeronPreviewIngressService;
import io.github.ike.ullmatcher.ha.aeron.AeronTransportMetrics;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

final class AeronPreviewReplicationTransportProvider implements ReplicationTransportProvider {
    private final GrpcReplicationTransportProvider grpcProvider;
    private final AeronPreviewIngressService ingressService;
    private final AeronTransportMetrics metrics;
    private final AeronPreviewReconciliationTracker reconciliationTracker;
    private final String previewChannel;
    private final int previewStreamId;

    AeronPreviewReplicationTransportProvider(ServerSecurityConfig securityConfig,
                                             String advertisedHost,
                                             AeronPreviewTransportConfig config,
                                             LongSupplier authoritativeLastReceivedSupplier) {
        this.grpcProvider = new GrpcReplicationTransportProvider(Objects.requireNonNull(securityConfig, "securityConfig"));
        Objects.requireNonNull(advertisedHost, "advertisedHost");
        Objects.requireNonNull(config, "config");
        this.reconciliationTracker = new AeronPreviewReconciliationTracker(
                Objects.requireNonNull(authoritativeLastReceivedSupplier, "authoritativeLastReceivedSupplier"));
        this.previewChannel = channel(advertisedHost, config.port());
        this.previewStreamId = config.streamId();
        this.metrics = new AeronTransportMetrics();
        this.ingressService = new AeronPreviewIngressService(
                previewChannel,
                previewStreamId,
                config.directory(),
                metrics,
                reconciliationTracker::recordPreviewSequence
        );
    }

    @Override
    public ReplicationTransportType type() {
        return ReplicationTransportType.AERON_PREVIEW;
    }

    @Override
    public Map<String, String> localNodeMetadata() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(TRANSPORT_METADATA_KEY, type().name());
        metadata.put(AERON_CHANNEL_METADATA_KEY, previewChannel);
        metadata.put(AERON_STREAM_ID_METADATA_KEY, Integer.toString(previewStreamId));
        return Map.copyOf(metadata);
    }

    @Override
    public ClusterPeerClient connect(DiscoveredNode node) throws IOException {
        ClusterPeerClient grpcClient = grpcProvider.connect(node);
        if (!(grpcClient instanceof GrpcClusterPeerClient grpcPeer)) {
            return grpcClient;
        }
        String channel = node.metadata().get(AERON_CHANNEL_METADATA_KEY);
        String streamId = node.metadata().get(AERON_STREAM_ID_METADATA_KEY);
        if (channel == null || channel.isBlank() || streamId == null || streamId.isBlank()) {
            return grpcClient;
        }
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(ingressService.aeronDirectoryName()));
        return new AeronPreviewClusterPeerClient(grpcPeer.delegate(), aeron, channel, Integer.parseInt(streamId), metrics);
    }

    @Override
    public TransportMetricsSnapshot metricsSnapshot() {
        AeronTransportMetrics.Snapshot snapshot = metrics.snapshot();
        return reconciliationTracker.enrich(new TransportMetricsSnapshot(
                type().name(),
                snapshot.previewPublishedCommands(),
                snapshot.previewPublishedBytes(),
                snapshot.previewPublishFailures(),
                snapshot.previewReceivedCommands(),
                snapshot.previewReceivedBytes(),
                snapshot.snapshotRequests(),
                snapshot.snapshotRequestFailures(),
                snapshot.snapshotBytesSent(),
                snapshot.snapshotBytesReceived(),
                snapshot.controlRequests(),
                snapshot.controlRequestFailures(),
                0L,
                0L,
                0L,
                0L,
                "IDLE",
                "no preview reconciliation state yet",
                "STABLE",
                "transport policy is stable"
        ));
    }

    @Override
    public void close() throws IOException {
        grpcProvider.close();
        ingressService.close();
    }

    private static String channel(String host, int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }
}
