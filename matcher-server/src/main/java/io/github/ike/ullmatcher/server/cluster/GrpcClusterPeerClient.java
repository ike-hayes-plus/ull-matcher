package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.ClusterPeerClient;
import io.github.ike.ullmatcher.ha.transport.AsyncBatchReplicationTarget;
import io.github.ike.ullmatcher.ha.grpc.client.GrpcReplicationTarget;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncResult;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.api.Command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

final class GrpcClusterPeerClient implements ClusterPeerClient, AsyncBatchReplicationTarget {
    private final GrpcReplicationTarget delegate;

    GrpcClusterPeerClient(GrpcReplicationTarget delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public String nodeId() {
        return delegate.nodeId();
    }

    @Override
    public void replicate(Command command, long timeoutNanos) throws IOException {
        delegate.replicate(command, timeoutNanos);
    }

    @Override
    public void replicateBatch(List<Command> commands, long timeoutNanos) throws IOException {
        delegate.replicateBatch(commands, timeoutNanos);
    }

    @Override
    public CompletableFuture<Void> replicateBatchAsync(List<Command> commands, long timeoutNanos) {
        return delegate.replicateBatchAsync(commands, timeoutNanos);
    }

    @Override
    public NodeControlState fetchNodeState(long timeoutNanos) throws IOException {
        return delegate.fetchNodeState(timeoutNanos);
    }

    @Override
    public SnapshotSyncResult downloadLatestSnapshot(Path targetFile, long timeoutNanos) throws IOException {
        return delegate.downloadLatestSnapshot(targetFile, timeoutNanos);
    }

    @Override
    public void close() {
        delegate.close();
    }

    GrpcReplicationTarget delegate() {
        return delegate;
    }
}
