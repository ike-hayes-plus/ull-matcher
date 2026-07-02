package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.ClusterPeerClient;
import io.aeron.Aeron;
import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.ha.aeron.AeronShadowReplicationTarget;
import io.github.ike.ullmatcher.ha.grpc.client.GrpcReplicationTarget;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncResult;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

final class AeronPreviewClusterPeerClient implements ClusterPeerClient {
    private static final Logger LOG = LoggerFactory.getLogger(AeronPreviewClusterPeerClient.class);

    private final GrpcReplicationTarget authoritativeTarget;
    private final AeronShadowReplicationTarget previewTarget;

    AeronPreviewClusterPeerClient(GrpcReplicationTarget authoritativeTarget, Aeron aeron, String channel, int streamId,
                                  io.github.ike.ullmatcher.ha.aeron.AeronTransportMetrics metrics) {
        this.authoritativeTarget = Objects.requireNonNull(authoritativeTarget, "authoritativeTarget");
        this.previewTarget = new AeronShadowReplicationTarget(
                Objects.requireNonNull(aeron, "aeron"),
                Objects.requireNonNull(channel, "channel"),
                streamId,
                Objects.requireNonNull(metrics, "metrics")
        );
    }

    @Override
    public String nodeId() {
        return authoritativeTarget.nodeId();
    }

    @Override
    public void replicate(Command command, long timeoutNanos) throws IOException {
        authoritativeTarget.replicate(command, timeoutNanos);
        try {
            previewTarget.publish(command, timeoutNanos);
        } catch (IOException e) {
            LOG.warn("Aeron preview publish failed for nodeId={} message={}", nodeId(), e.getMessage());
        }
    }

    @Override
    public NodeControlState fetchNodeState(long timeoutNanos) throws IOException {
        return authoritativeTarget.fetchNodeState(timeoutNanos);
    }

    @Override
    public SnapshotSyncResult downloadLatestSnapshot(Path targetFile, long timeoutNanos) throws IOException {
        return authoritativeTarget.downloadLatestSnapshot(targetFile, timeoutNanos);
    }

    @Override
    public void close() {
        try {
            previewTarget.close();
        } finally {
            authoritativeTarget.close();
        }
    }
}
