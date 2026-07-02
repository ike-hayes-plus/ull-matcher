package io.github.ike.ullmatcher.ha.grpc.client;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.state.NodeControlStateClient;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursorSource;
import io.github.ike.ullmatcher.ha.replication.ReplicationStream;
import io.github.ike.ullmatcher.ha.replication.ReplicationTarget;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncResult;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncSource;
import io.github.ike.ullmatcher.ha.replication.StreamingReplicationTarget;
import io.github.ike.ullmatcher.ha.grpc.codec.ProtoAdapters;
import io.github.ike.ullmatcher.ha.grpc.proto.CursorRequest;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationBatchAck;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationBatchRequest;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationServiceGrpc;
import io.github.ike.ullmatcher.ha.grpc.proto.SnapshotRequest;
import io.github.ike.ullmatcher.ha.grpc.security.GrpcClientTlsConfig;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class GrpcReplicationTarget implements ReplicationTarget, ReplicationCursorSource, NodeControlStateClient,
        SnapshotSyncSource, StreamingReplicationTarget, Closeable {
    private static final int STREAM_MAX_BATCH_COMMANDS = 256;
    private static final int STREAM_MAX_BATCH_BYTES = 512 << 10;

    private final String nodeId;
    private final ManagedChannel channel;
    private final GrpcTransportMetrics metrics;
    private final ReplicationServiceGrpc.ReplicationServiceBlockingStub blockingStub;
    private final ReplicationServiceGrpc.ReplicationServiceStub asyncStub;

    public GrpcReplicationTarget(String nodeId, ManagedChannel channel) {
        this(nodeId, channel, new GrpcTransportMetrics());
    }

    public GrpcReplicationTarget(String nodeId, ManagedChannel channel, GrpcTransportMetrics metrics) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.blockingStub = ReplicationServiceGrpc.newBlockingStub(channel);
        this.asyncStub = ReplicationServiceGrpc.newStub(channel);
    }

    public static GrpcReplicationTarget connect(String nodeId, GrpcReplicationClientConfig config) {
        Objects.requireNonNull(config, "config");
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(config.host(), config.port())
                .maxInboundMessageSize(config.maxInboundMessageSize())
                .keepAliveTime(60L, TimeUnit.SECONDS)
                .keepAliveTimeout(5L, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(false)
                .withOption(ChannelOption.TCP_NODELAY, true);
        if (config.tls().plaintext()) {
            builder.usePlaintext();
        } else {
            builder.sslContext(clientSslContext(config.tls()));
            if (!config.tls().authorityOverride().isBlank()) {
                builder.overrideAuthority(config.tls().authorityOverride());
            }
        }
        return new GrpcReplicationTarget(nodeId, builder.build());
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public void replicate(Command command, long timeoutNanos) throws IOException {
        Objects.requireNonNull(command, "command");
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        try {
            blocking(timeoutNanos).replicate(ProtoAdapters.toProto(command));
            metrics.recordUnaryReplication();
        } catch (StatusRuntimeException e) {
            metrics.recordFailure();
            throw new IOException("gRPC replication to standby " + nodeId + " failed: " + e.getStatus(), e);
        }
    }

    @Override
    public void replicateBatch(List<Command> commands, long timeoutNanos) throws IOException {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return;
        }
        try (ReplicationStream stream = openReplicationStream(timeoutNanos)) {
            for (Command command : commands) {
                stream.replicate(command);
            }
            stream.closeAndAwaitCursor();
        } catch (StatusRuntimeException e) {
            metrics.recordFailure();
            throw new IOException("gRPC replication batch to standby " + nodeId + " failed: " + e.getStatus(), e);
        }
    }

    public CompletableFuture<Void> replicateBatchAsync(List<Command> commands, long timeoutNanos) {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            GrpcStream stream = new GrpcStream(timeoutNanos);
            for (Command command : commands) {
                stream.replicate(command);
            }
            return stream.closeAsync().thenApply(ignored -> null);
        } catch (IOException e) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    @Override
    public ReplicationCursor fetchCursor(long timeoutNanos) throws IOException {
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        try {
            return ProtoAdapters.fromProto(blocking(timeoutNanos).fetchCursor(CursorRequest.getDefaultInstance()));
        } catch (StatusRuntimeException e) {
            metrics.recordFailure();
            throw new IOException("failed to fetch standby cursor from " + nodeId + ": " + e.getStatus(), e);
        }
    }

    @Override
    public NodeControlState fetchNodeState(long timeoutNanos) throws IOException {
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        try {
            return ProtoAdapters.fromProto(blocking(timeoutNanos).fetchNodeState(CursorRequest.getDefaultInstance()));
        } catch (StatusRuntimeException e) {
            metrics.recordFailure();
            throw new IOException("failed to fetch node state from " + nodeId + ": " + e.getStatus(), e);
        }
    }

    @Override
    public SnapshotSyncResult downloadLatestSnapshot(Path targetFile, long timeoutNanos) throws IOException {
        Objects.requireNonNull(targetFile, "targetFile");
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        Path parent = targetFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = targetFile.resolveSibling(targetFile.getFileName() + ".part");
        long bytesWritten = 0L;
        long lastSequence = 0L;
        long lastTradeId = 0L;
        long liveOrderCount = 0L;
        int expectedChunk = 0;
        try (OutputStream out = Files.newOutputStream(tmp)) {
            var iterator = blocking(timeoutNanos).downloadLatestSnapshot(SnapshotRequest.getDefaultInstance());
            while (iterator.hasNext()) {
                var chunk = iterator.next();
                if (chunk.getChunkIndex() != expectedChunk++) {
                    throw new IOException("snapshot chunk out of order from " + nodeId);
                }
                byte[] payload = chunk.getPayload().toByteArray();
                out.write(payload);
                bytesWritten += payload.length;
                metrics.recordSnapshotBytesReceived(payload.length);
                lastSequence = chunk.getLastSequence();
                lastTradeId = chunk.getLastTradeId();
                liveOrderCount = chunk.getLiveOrderCount();
            }
        } catch (StatusRuntimeException e) {
            Files.deleteIfExists(tmp);
            metrics.recordFailure();
            throw new IOException("failed to download snapshot from " + nodeId + ": " + e.getStatus(), e);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
        Files.move(tmp, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return new SnapshotSyncResult(targetFile, bytesWritten, lastSequence, lastTradeId, liveOrderCount);
    }

    @Override
    public ReplicationStream openReplicationStream(long timeoutNanos) throws IOException {
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        return new GrpcStream(timeoutNanos);
    }

    @Override
    public void close() {
        channel.shutdown();
    }

    private ReplicationServiceGrpc.ReplicationServiceBlockingStub blocking(long timeoutNanos) {
        return timeoutNanos == 0L
                ? blockingStub
                : blockingStub.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private ReplicationServiceGrpc.ReplicationServiceStub async(long timeoutNanos) {
        return timeoutNanos == 0L
                ? asyncStub
                : asyncStub.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private static io.grpc.netty.shaded.io.netty.handler.ssl.SslContext clientSslContext(GrpcClientTlsConfig tls) {
        try {
            SslContextBuilder builder = GrpcSslContexts.configure(SslContextBuilder.forClient())
                    .trustManager(Files.newInputStream(tls.trustCertCollectionFile()));
            if (tls.certificateChainFile() != null) {
                builder.keyManager(
                        Files.newInputStream(tls.certificateChainFile()),
                        Files.newInputStream(tls.privateKeyFile())
                );
            }
            return builder.build();
        } catch (IOException e) {
            throw new IllegalStateException("failed to build gRPC client TLS context", e);
        } catch (Exception e) {
            throw new IllegalStateException("failed to initialize gRPC client TLS", e);
        }
    }

    private final class GrpcStream implements ReplicationStream {
        private final AtomicReference<ReplicationCursor> lastAck = new AtomicReference<>(new ReplicationCursor(0L, 0L, 0L, 0L));
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final CompletableFuture<ReplicationCursor> completion = new CompletableFuture<>();
        private final StreamObserver<ReplicationBatchRequest> requestObserver;
        private final int maxBatchCommands;
        private final int maxBatchBytes;
        private final java.util.ArrayList<io.github.ike.ullmatcher.ha.grpc.proto.CommandEnvelope> pendingBatch;
        private int pendingBytes;
        private long nextBatchId = 1L;
        private volatile boolean closed;

        private GrpcStream(long timeoutNanos) {
            this.maxBatchCommands = STREAM_MAX_BATCH_COMMANDS;
            this.maxBatchBytes = STREAM_MAX_BATCH_BYTES;
            this.pendingBatch = new java.util.ArrayList<>(maxBatchCommands);
            this.requestObserver = async(timeoutNanos).openReplicationStream(
                    new StreamObserver<>() {
                        @Override
                        public void onNext(ReplicationBatchAck value) {
                            lastAck.set(ProtoAdapters.fromProto(value.getCursor()));
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            failure.set(throwable);
                            completion.completeExceptionally(throwable);
                        }

                        @Override
                        public void onCompleted() {
                            completion.complete(lastAck.get());
                        }
                    }
            );
        }

        @Override
        public void replicate(Command command) throws IOException {
            Objects.requireNonNull(command, "command");
            ensureOpen();
            var encoded = ProtoAdapters.toProto(command);
            pendingBatch.add(encoded);
            pendingBytes += encoded.getSerializedSize();
            if (pendingBatch.size() >= maxBatchCommands || pendingBytes >= maxBatchBytes) {
                flushBatch();
            }
            throwIfFailed();
        }

        @Override
        public ReplicationCursor lastAckedCursor() {
            return lastAck.get();
        }

        @Override
        public ReplicationCursor closeAndAwaitCursor() throws IOException {
            try {
                return closeAsync().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for replication stream completion", e);
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException io) {
                    throw io;
                }
                throw new IOException("gRPC replication stream to standby " + nodeId + " failed", cause);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                try {
                    flushBatch();
                } catch (IOException ignored) {
                }
                closed = true;
                requestObserver.onCompleted();
            }
        }

        private CompletableFuture<ReplicationCursor> closeAsync() throws IOException {
            if (!closed) {
                flushBatch();
                closed = true;
                requestObserver.onCompleted();
            }
            return completion.whenComplete((cursor, error) -> {
                if (error != null) {
                    metrics.recordFailure();
                }
            });
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("replication stream already closed");
            }
            throwIfFailed();
        }

        private void flushBatch() throws IOException {
            if (pendingBatch.isEmpty()) {
                return;
            }
            requestObserver.onNext(ReplicationBatchRequest.newBuilder()
                    .setBatchId(nextBatchId++)
                    .addAllCommands(pendingBatch)
                    .build());
            metrics.recordStreamBatch(pendingBatch.size());
            pendingBatch.clear();
            pendingBytes = 0;
        }

        private void throwIfFailed() throws IOException {
            Throwable throwable = failure.get();
            if (throwable == null) {
                return;
            }
            if (throwable instanceof StatusRuntimeException status) {
                metrics.recordFailure();
                throw new IOException("gRPC replication stream to standby " + nodeId + " failed: " + status.getStatus(), status);
            }
            throw new IOException("gRPC replication stream to standby " + nodeId + " failed", throwable);
        }
    }

    public GrpcTransportMetrics metrics() {
        return metrics;
    }
}
