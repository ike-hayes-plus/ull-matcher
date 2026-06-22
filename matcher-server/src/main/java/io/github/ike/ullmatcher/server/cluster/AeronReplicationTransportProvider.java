package io.github.ike.ullmatcher.server.cluster;

import io.aeron.Aeron;
import io.github.ike.ullmatcher.ha.aeron.AeronTransportMetrics;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterialSource;
import io.github.ike.ullmatcher.ha.state.NodeControlStateSource;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.server.security.ReloadableTransportSecurityContext;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.server.security.TlsReloadSnapshot;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

final class AeronReplicationTransportProvider implements ReplicationTransportProvider {
    private final String localNodeId;
    private final AeronAuthoritativeIngressService ingressService;
    private final AeronTransportMetrics metrics;
    private final String commandChannel;
    private final int commandStreamId;
    private final String snapshotRequestChannel;
    private final int snapshotRequestStreamId;
    private final String localSnapshotResponseChannel;
    private final int localSnapshotResponseStreamId;
    private final String controlRequestChannel;
    private final int controlRequestStreamId;
    private final String localControlResponseChannel;
    private final int localControlResponseStreamId;
    private final String localCommandAckChannel;
    private final int localCommandAckStreamId;
    private final String securityHandshakeRequestChannel;
    private final int securityHandshakeRequestStreamId;
    private final String localSecurityHandshakeResponseChannel;
    private final int localSecurityHandshakeResponseStreamId;
    private final ReloadableTransportSecurityContext securityContext;
    private final ConcurrentHashMap<String, LocalPeerResponseEndpoints> peerResponseEndpoints = new ConcurrentHashMap<>();
    private final AtomicInteger nextPeerEndpointOffset = new AtomicInteger();

    AeronReplicationTransportProvider(ServerSecurityConfig securityConfig,
                                      String localNodeId,
                                      String advertisedHost,
                                      AeronPreviewTransportConfig config,
                                      Supplier<StandbySyncService> standbySyncServiceSupplier,
                                      SnapshotMaterialSource snapshotMaterialSource,
                                      NodeControlStateSource nodeControlStateSource) {
        Objects.requireNonNull(securityConfig, "securityConfig");
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        Objects.requireNonNull(advertisedHost, "advertisedHost");
        Objects.requireNonNull(config, "config");
        this.commandChannel = config.commandChannel(advertisedHost);
        this.commandStreamId = config.streamId();
        this.snapshotRequestChannel = config.snapshotRequestChannel(advertisedHost);
        this.snapshotRequestStreamId = config.snapshotRequestStreamId();
        this.localSnapshotResponseChannel = config.snapshotResponseChannel(advertisedHost);
        this.localSnapshotResponseStreamId = config.snapshotResponseStreamId();
        this.controlRequestChannel = config.controlRequestChannel(advertisedHost);
        this.controlRequestStreamId = config.controlRequestStreamId();
        this.localControlResponseChannel = config.controlResponseChannel(advertisedHost);
        this.localControlResponseStreamId = config.controlResponseStreamId();
        this.localCommandAckChannel = config.commandAckChannel(advertisedHost);
        this.localCommandAckStreamId = config.commandAckStreamId();
        this.securityHandshakeRequestChannel = config.securityHandshakeRequestChannel(advertisedHost);
        this.securityHandshakeRequestStreamId = config.securityHandshakeRequestStreamId();
        this.localSecurityHandshakeResponseChannel = config.securityHandshakeResponseChannel(advertisedHost);
        this.localSecurityHandshakeResponseStreamId = config.securityHandshakeResponseStreamId();
        this.metrics = new AeronTransportMetrics();
        this.securityContext = newSecurityContext(securityConfig);
        this.ingressService = new AeronAuthoritativeIngressService(
                localNodeId,
                commandChannel,
                commandStreamId,
                snapshotRequestChannel,
                snapshotRequestStreamId,
                controlRequestChannel,
                controlRequestStreamId,
                securityHandshakeRequestChannel,
                securityHandshakeRequestStreamId,
                config.directory(),
                metrics,
                standbySyncServiceSupplier,
                snapshotMaterialSource,
                nodeControlStateSource,
                securityContext
        );
    }

    @Override
    public ReplicationTransportType type() {
        return ReplicationTransportType.AERON;
    }

    @Override
    public Map<String, String> localNodeMetadata() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(TRANSPORT_METADATA_KEY, type().name());
        metadata.put(AERON_CHANNEL_METADATA_KEY, commandChannel);
        metadata.put(AERON_STREAM_ID_METADATA_KEY, Integer.toString(commandStreamId));
        metadata.put(AERON_SNAPSHOT_REQUEST_CHANNEL_METADATA_KEY, snapshotRequestChannel);
        metadata.put(AERON_SNAPSHOT_REQUEST_STREAM_ID_METADATA_KEY, Integer.toString(snapshotRequestStreamId));
        metadata.put(AERON_CONTROL_REQUEST_CHANNEL_METADATA_KEY, controlRequestChannel);
        metadata.put(AERON_CONTROL_REQUEST_STREAM_ID_METADATA_KEY, Integer.toString(controlRequestStreamId));
        metadata.put(AERON_SECURITY_HANDSHAKE_REQUEST_CHANNEL_METADATA_KEY, securityHandshakeRequestChannel);
        metadata.put(AERON_SECURITY_HANDSHAKE_REQUEST_STREAM_ID_METADATA_KEY, Integer.toString(securityHandshakeRequestStreamId));
        return Map.copyOf(metadata);
    }

    @Override
    public ClusterPeerClient connect(DiscoveredNode node) throws IOException {
        String remoteChannel = node.metadata().get(AERON_CHANNEL_METADATA_KEY);
        String remoteStreamId = node.metadata().get(AERON_STREAM_ID_METADATA_KEY);
        String remoteSnapshotRequestChannel = node.metadata().get(AERON_SNAPSHOT_REQUEST_CHANNEL_METADATA_KEY);
        String remoteSnapshotRequestStreamId = node.metadata().get(AERON_SNAPSHOT_REQUEST_STREAM_ID_METADATA_KEY);
        String remoteControlRequestChannel = node.metadata().get(AERON_CONTROL_REQUEST_CHANNEL_METADATA_KEY);
        String remoteControlRequestStreamId = node.metadata().get(AERON_CONTROL_REQUEST_STREAM_ID_METADATA_KEY);
        String remoteSecurityHandshakeRequestChannel = node.metadata().get(AERON_SECURITY_HANDSHAKE_REQUEST_CHANNEL_METADATA_KEY);
        String remoteSecurityHandshakeRequestStreamId = node.metadata().get(AERON_SECURITY_HANDSHAKE_REQUEST_STREAM_ID_METADATA_KEY);
        if (remoteChannel == null || remoteChannel.isBlank()
                || remoteStreamId == null || remoteStreamId.isBlank()
                || remoteSnapshotRequestChannel == null || remoteSnapshotRequestChannel.isBlank()
                || remoteSnapshotRequestStreamId == null || remoteSnapshotRequestStreamId.isBlank()
                || remoteControlRequestChannel == null || remoteControlRequestChannel.isBlank()
                || remoteControlRequestStreamId == null || remoteControlRequestStreamId.isBlank()
                || (securityContext != null && (remoteSecurityHandshakeRequestChannel == null || remoteSecurityHandshakeRequestChannel.isBlank()
                || remoteSecurityHandshakeRequestStreamId == null || remoteSecurityHandshakeRequestStreamId.isBlank()))) {
            throw new IOException("remote node " + node.nodeId() + " does not advertise Aeron transport metadata");
        }
        LocalPeerResponseEndpoints localEndpoints = peerResponseEndpoints.computeIfAbsent(
                node.nodeId(),
                ignored -> allocatePeerResponseEndpoints());
        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(ingressService.aeronDirectoryName()));
        return new AeronAuthoritativeClusterPeerClient(
                localNodeId,
                node.nodeId(),
                aeron,
                remoteChannel,
                Integer.parseInt(remoteStreamId),
                remoteSnapshotRequestChannel,
                Integer.parseInt(remoteSnapshotRequestStreamId),
                localEndpoints.snapshotResponseChannel(),
                localEndpoints.snapshotResponseStreamId(),
                remoteControlRequestChannel,
                Integer.parseInt(remoteControlRequestStreamId),
                localEndpoints.controlResponseChannel(),
                localEndpoints.controlResponseStreamId(),
                localEndpoints.commandAckChannel(),
                localEndpoints.commandAckStreamId(),
                metrics,
                securityContext,
                remoteSecurityHandshakeRequestChannel,
                remoteSecurityHandshakeRequestChannel == null ? 0 : Integer.parseInt(remoteSecurityHandshakeRequestStreamId),
                localEndpoints.securityHandshakeResponseChannel(),
                localEndpoints.securityHandshakeResponseStreamId()
        );
    }

    @Override
    public TransportMetricsSnapshot metricsSnapshot() {
        AeronTransportMetrics.Snapshot snapshot = metrics.snapshot();
        return new TransportMetricsSnapshot(
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
                "DISABLED",
                "authoritative Aeron mode does not use preview reconciliation",
                "STABLE",
                "transport policy is stable"
        );
    }

    @Override
    public TlsReloadSnapshot securitySnapshot() {
        return securityContext == null ? ReplicationTransportProvider.super.securitySnapshot() : securityContext.snapshot();
    }

    @Override
    public void close() throws IOException {
        IOException error = null;
        try {
            ingressService.close();
        } catch (IOException e) {
            error = e;
        }
        if (securityContext != null) {
            try {
                securityContext.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }

    private static ReloadableTransportSecurityContext newSecurityContext(ServerSecurityConfig securityConfig) {
        if (!securityConfig.transportSecurityEnabled()) {
            return null;
        }
        try {
            return new ReloadableTransportSecurityContext(securityConfig);
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("failed to initialize Aeron transport security context", e);
        }
    }

    private LocalPeerResponseEndpoints allocatePeerResponseEndpoints() {
        int offset = nextPeerEndpointOffset.getAndIncrement();
        return new LocalPeerResponseEndpoints(
                localSnapshotResponseChannel,
                localSnapshotResponseStreamId + offset,
                localControlResponseChannel,
                localControlResponseStreamId + offset,
                localCommandAckChannel,
                localCommandAckStreamId + offset,
                localSecurityHandshakeResponseChannel,
                localSecurityHandshakeResponseStreamId + offset
        );
    }

    private record LocalPeerResponseEndpoints(
            String snapshotResponseChannel,
            int snapshotResponseStreamId,
            String controlResponseChannel,
            int controlResponseStreamId,
            String commandAckChannel,
            int commandAckStreamId,
            String securityHandshakeResponseChannel,
            int securityHandshakeResponseStreamId
    ) {
    }
}
