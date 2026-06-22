package io.github.ike.ullmatcher.ha.grpc.server;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.ha.grpc.codec.ProtoAdapters;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.state.NodeControlStateSource;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterialSource;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.ha.grpc.proto.CommandEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.CursorRequest;
import io.github.ike.ullmatcher.ha.grpc.proto.NodeControlStateEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationBatchAck;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationBatchRequest;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationCursorEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationServiceGrpc;
import io.github.ike.ullmatcher.ha.grpc.proto.SnapshotChunkEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.SnapshotRequest;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public final class GrpcReplicationService extends ReplicationServiceGrpc.ReplicationServiceImplBase {
    private static final int SNAPSHOT_CHUNK_SIZE = 64 * 1024;
    private static final long DEFAULT_REPLICATION_INGRESS_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(2);
    private static final int STREAM_FLUSH_COMMAND_THRESHOLD = 128;
    private static final long STREAM_FLUSH_MAX_DELAY_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    private final Supplier<StandbySyncService> standbySyncServiceSupplier;
    private final NodeControlStateSource nodeControlStateSource;
    private final SnapshotMaterialSource snapshotMaterialSource;
    private final GrpcTransportMetrics metrics;
    private final long replicationIngressTimeoutNanos;

    public GrpcReplicationService(StandbySyncService standbySyncService) {
        this(
                () -> Objects.requireNonNull(standbySyncService, "standbySyncService"),
                () -> { throw new IllegalStateException("nodeControlStateSource not configured"); },
                () -> {
                    throw new IOException("snapshot source not configured");
                },
                new GrpcTransportMetrics(),
                DEFAULT_REPLICATION_INGRESS_TIMEOUT_NANOS
        );
    }

    public GrpcReplicationService(StandbySyncService standbySyncService,
                                  NodeControlStateSource nodeControlStateSource,
                                  SnapshotMaterialSource snapshotMaterialSource) {
        this(() -> Objects.requireNonNull(standbySyncService, "standbySyncService"), nodeControlStateSource, snapshotMaterialSource,
                new GrpcTransportMetrics(), DEFAULT_REPLICATION_INGRESS_TIMEOUT_NANOS);
    }

    public GrpcReplicationService(Supplier<StandbySyncService> standbySyncServiceSupplier,
                                  NodeControlStateSource nodeControlStateSource,
                                  SnapshotMaterialSource snapshotMaterialSource,
                                  GrpcTransportMetrics metrics) {
        this(standbySyncServiceSupplier, nodeControlStateSource, snapshotMaterialSource, metrics,
                DEFAULT_REPLICATION_INGRESS_TIMEOUT_NANOS);
    }

    public GrpcReplicationService(Supplier<StandbySyncService> standbySyncServiceSupplier,
                                  NodeControlStateSource nodeControlStateSource,
                                  SnapshotMaterialSource snapshotMaterialSource,
                                  GrpcTransportMetrics metrics,
                                  long replicationIngressTimeoutNanos) {
        this.standbySyncServiceSupplier = Objects.requireNonNull(standbySyncServiceSupplier, "standbySyncServiceSupplier");
        this.nodeControlStateSource = Objects.requireNonNull(nodeControlStateSource, "nodeControlStateSource");
        this.snapshotMaterialSource = Objects.requireNonNull(snapshotMaterialSource, "snapshotMaterialSource");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        if (replicationIngressTimeoutNanos <= 0L) {
            throw new IllegalArgumentException("replicationIngressTimeoutNanos must be positive");
        }
        this.replicationIngressTimeoutNanos = replicationIngressTimeoutNanos;
    }

    @Override
    public void replicate(CommandEnvelope commandEnvelope, StreamObserver<ReplicationCursorEnvelope> observer) {
        try {
            ensureReplicationIngressAllowed();
            StandbySyncService standbySyncService = standbySyncServiceSupplier.get();
            Command command = ProtoAdapters.fromProto(commandEnvelope);
            standbySyncService.replicate(command, replicationIngressTimeoutNanos);
            metrics.recordUnaryReplication();
            observer.onNext(ProtoAdapters.toProto(standbySyncService.cursor()));
            observer.onCompleted();
        } catch (IOException e) {
            metrics.recordFailure();
            observer.onError(Status.INTERNAL.withDescription("standby replication failed").withCause(e).asRuntimeException());
        } catch (RuntimeException e) {
            metrics.recordRejectedIngress();
            observer.onError(Status.FAILED_PRECONDITION.withDescription("standby rejected replicated command: " + e.getMessage()).withCause(e).asRuntimeException());
        }
    }

    @Override
    public void fetchCursor(CursorRequest ignored, StreamObserver<ReplicationCursorEnvelope> observer) {
        observer.onNext(ProtoAdapters.toProto(standbySyncServiceSupplier.get().cursor()));
        observer.onCompleted();
    }

    @Override
    public StreamObserver<ReplicationBatchRequest> openReplicationStream(StreamObserver<ReplicationBatchAck> observer) {
        try {
            ensureReplicationIngressAllowed();
        } catch (IllegalStateException e) {
            metrics.recordRejectedIngress();
            observer.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asRuntimeException());
            return new StreamObserver<>() {
                @Override
                public void onNext(ReplicationBatchRequest value) {}

                @Override
                public void onError(Throwable throwable) {}

                @Override
                public void onCompleted() {}
            };
        }
        if (observer instanceof ServerCallStreamObserver<ReplicationBatchAck> serverObserver) {
            serverObserver.setCompression("identity");
        }
        AtomicBoolean terminated = new AtomicBoolean(false);
        return new StreamObserver<>() {
            private final java.util.ArrayList<PendingStreamAck> pendingAcks = new java.util.ArrayList<>();
            private int pendingCommands;
            private long firstPendingNanos;

            @Override
            public void onNext(ReplicationBatchRequest request) {
                try {
                    ensureReplicationIngressAllowed();
                    StandbySyncService standbySyncService = standbySyncServiceSupplier.get();
                    java.util.ArrayList<Command> commands = new java.util.ArrayList<>(request.getCommandsCount());
                    for (CommandEnvelope envelope : request.getCommandsList()) {
                        commands.add(ProtoAdapters.fromProto(envelope));
                    }
                    standbySyncService.appendReplicatedBatch(commands, replicationIngressTimeoutNanos);
                    int acked = commands.size();
                    metrics.recordStreamBatch(acked);
                    if (pendingCommands == 0) {
                        firstPendingNanos = System.nanoTime();
                    }
                    pendingCommands += acked;
                    pendingAcks.add(new PendingStreamAck(request.getBatchId(), acked));
                    if (pendingCommands >= STREAM_FLUSH_COMMAND_THRESHOLD
                            || System.nanoTime() - firstPendingNanos >= STREAM_FLUSH_MAX_DELAY_NANOS) {
                        flushPendingAcks(standbySyncService);
                    }
                } catch (IOException e) {
                    metrics.recordFailure();
                    if (terminated.compareAndSet(false, true)) {
                        observer.onError(Status.INTERNAL.withDescription("standby stream replication failed").withCause(e).asRuntimeException());
                    }
                } catch (RuntimeException e) {
                    metrics.recordRejectedIngress();
                    if (terminated.compareAndSet(false, true)) {
                        observer.onError(Status.FAILED_PRECONDITION.withDescription("standby rejected streamed command: " + e.getMessage()).withCause(e).asRuntimeException());
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                metrics.recordFailure();
                if (terminated.compareAndSet(false, true)) {
                    observer.onError(throwable);
                }
            }

            @Override
            public void onCompleted() {
                if (terminated.compareAndSet(false, true)) {
                    try {
                        flushPendingAcks(standbySyncServiceSupplier.get());
                    } catch (IOException e) {
                        metrics.recordFailure();
                        observer.onError(Status.INTERNAL.withDescription("standby stream replication failed").withCause(e).asRuntimeException());
                        return;
                    }
                    observer.onCompleted();
                }
            }

            private void flushPendingAcks(StandbySyncService standbySyncService) throws IOException {
                if (pendingAcks.isEmpty()) {
                    return;
                }
                standbySyncService.force(pendingCommands);
                ReplicationCursorEnvelope cursor = ProtoAdapters.toProto(standbySyncService.cursor());
                for (PendingStreamAck pendingAck : pendingAcks) {
                    observer.onNext(ReplicationBatchAck.newBuilder()
                            .setBatchId(pendingAck.batchId())
                            .setAckedCommands(pendingAck.ackedCommands())
                            .setCursor(cursor)
                            .build());
                }
                pendingAcks.clear();
                pendingCommands = 0;
                firstPendingNanos = 0L;
            }
        };
    }

    private record PendingStreamAck(long batchId, int ackedCommands) {
    }

    @Override
    public void fetchNodeState(CursorRequest ignored, StreamObserver<NodeControlStateEnvelope> observer) {
        observer.onNext(ProtoAdapters.toProto(nodeControlStateSource.currentState()));
        observer.onCompleted();
    }

    @Override
    public void downloadLatestSnapshot(SnapshotRequest ignored, StreamObserver<SnapshotChunkEnvelope> observer) {
        try {
            SnapshotMaterial snapshot = snapshotMaterialSource.latestSnapshot();
            long totalBytes = Files.size(snapshot.file());
            byte[] buffer = new byte[SNAPSHOT_CHUNK_SIZE];
            int chunkIndex = 0;
            if (observer instanceof ServerCallStreamObserver<SnapshotChunkEnvelope> serverObserver) {
                serverObserver.setCompression("gzip");
            }
            try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(snapshot.file()))) {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    if (read == 0) {
                        continue;
                    }
                    byte[] payload = new byte[read];
                    System.arraycopy(buffer, 0, payload, 0, read);
                    boolean lastChunk = in.available() == 0;
                    observer.onNext(ProtoAdapters.snapshotChunk(
                            snapshot.lastSequence(),
                            snapshot.lastTradeId(),
                            snapshot.liveOrderCount(),
                            totalBytes,
                            chunkIndex++,
                            lastChunk,
                            payload
                    ));
                    metrics.recordSnapshotBytesSent(read);
                }
            }
            if (chunkIndex == 0) {
                observer.onNext(ProtoAdapters.snapshotChunk(
                        snapshot.lastSequence(),
                        snapshot.lastTradeId(),
                        snapshot.liveOrderCount(),
                        totalBytes,
                        0,
                        true,
                        new byte[0]
                ));
            }
            observer.onCompleted();
        } catch (IOException e) {
            metrics.recordFailure();
            observer.onError(Status.INTERNAL.withDescription("snapshot streaming failed").withCause(e).asRuntimeException());
        }
    }

    private void ensureReplicationIngressAllowed() {
        switch (nodeControlStateSource.currentState().role()) {
            case STANDBY, CATCHING_UP -> {
                return;
            }
            default -> throw new IllegalStateException("node is not accepting standby replication ingress");
        }
    }

    public GrpcTransportMetrics metrics() {
        return metrics;
    }
}
