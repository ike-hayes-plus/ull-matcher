package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 基于发现结果维护复制目标集合，并按复制策略执行 fan-out。
 * <p>
 * 它把发现层转成可用复制 target 集合，并按 any / quorum / all 语义推进复制完成条件。
 */
final class DiscoveryDrivenReplicator implements CommandReplicator, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryDrivenReplicator.class);
    private static final int DEFAULT_PREFERRED_BATCH_SIZE = 2_048;
    private static final int DEFAULT_PREFERRED_IN_FLIGHT_BATCHES = 16;
    private static final long DEFAULT_PREFERRED_ACCUMULATION_NANOS = TimeUnit.MICROSECONDS.toNanos(200);
    private static final int AERON_MULTI_STANDBY_BATCH_SIZE = 256;
    private static final int AERON_SINGLE_STANDBY_BATCH_SIZE = 256;
    private static final int AERON_MULTI_STANDBY_IN_FLIGHT_BATCHES = 16;
    private static final long AERON_MULTI_STANDBY_ACCUMULATION_NANOS = 0L;

    private final String localNodeId;
    private final ReplicationTransportProvider transportProvider;
    private final Map<String, TargetRef> targets = new LinkedHashMap<>();
    private final ExecutorService asyncExecutor;

    DiscoveryDrivenReplicator(String localNodeId, ReplicationTransportProvider transportProvider) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.transportProvider = Objects.requireNonNull(transportProvider, "transportProvider");
        this.asyncExecutor = Executors.newFixedThreadPool(
                Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors())),
                Thread.ofPlatform().name("matcher-replication-async-", 0).factory()
        );
    }

    /**
     * 用最新发现结果刷新复制目标集合。
     */
    synchronized void refresh(List<DiscoveredNode> nodes) throws IOException {
        Map<String, TargetRef> next = new LinkedHashMap<>();
        for (DiscoveredNode node : nodes) {
            if (localNodeId.equals(node.nodeId())) {
                continue;
            }
            TargetRef existing = targets.get(node.nodeId());
            if (existing != null && existing.matches(node)) {
                next.put(node.nodeId(), existing);
            } else {
                closeQuietly(existing);
                LOG.info("refreshing replication target {} -> {}:{}", node.nodeId(), node.host(), node.grpcPort());
                next.put(node.nodeId(), new TargetRef(node.host(), node.grpcPort(), transportProvider.connect(node)));
            }
        }
        for (Map.Entry<String, TargetRef> entry : targets.entrySet()) {
            if (!next.containsKey(entry.getKey())) {
                closeQuietly(entry.getValue());
            }
        }
        targets.clear();
        targets.putAll(next);
    }

    @Override
    public synchronized ReplicationResult replicate(Command command, long timeoutNanos) throws IOException {
        return replicateBatch(List.of(Objects.requireNonNull(command, "command")), timeoutNanos);
    }

    @Override
    public synchronized ReplicationResult replicateBatch(List<Command> commands, long timeoutNanos) throws IOException {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return new ReplicationResult(targets.size(), 0, List.of(), List.of());
        }
        return replicateSequentially(commands, timeoutNanos, new ArrayList<>(targets.values()));
    }

    @Override
    public CompletableFuture<ReplicationResult> replicateBatchAsync(List<Command> commands, long timeoutNanos) throws IOException {
        return replicateBatchAsync(commands, ReplicationMode.WAIT_FOR_ALL_STANDBYS, timeoutNanos);
    }

    @Override
    /**
     * 异步批量复制，并按复制模式尽早完成。
     * <p>
     * `quorum` 或 `any` 满足条件后立即结束，不让慢副本无谓拖住 committed watermark。
     */
    public CompletableFuture<ReplicationResult> replicateBatchAsync(List<Command> commands,
                                                                    ReplicationMode mode,
                                                                    long timeoutNanos) throws IOException {
        Objects.requireNonNull(commands, "commands");
        Objects.requireNonNull(mode, "mode");
        List<TargetRef> targetSnapshot;
        synchronized (this) {
            targetSnapshot = List.copyOf(targets.values());
        }
        if (commands.isEmpty()) {
            return CompletableFuture.completedFuture(new ReplicationResult(targetSnapshot.size(), 0, List.of(), List.of()));
        }
        if (targetSnapshot.isEmpty()) {
            return CompletableFuture.completedFuture(new ReplicationResult(0, 0, List.of(), List.of()));
        }
        int requiredAcks = mode.requiredAcks(targetSnapshot.size());
        if (requiredAcks == 0) {
            return CompletableFuture.completedFuture(new ReplicationResult(targetSnapshot.size(), 0, List.of(), List.of()));
        }
        String[] acked = new String[targetSnapshot.size()];
        String[] failed = new String[targetSnapshot.size()];
        java.util.concurrent.atomic.AtomicInteger ackedCount = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger failedCount = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger completedCount = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean resolved = new java.util.concurrent.atomic.AtomicBoolean();
        CompletableFuture<ReplicationResult> result = new CompletableFuture<>();
        for (int i = 0; i < targetSnapshot.size(); i++) {
            TargetRef target = targetSnapshot.get(i);
            CompletableFuture<Void> future;
            if (target.target() instanceof AsyncBatchReplicationTarget asyncTarget) {
                future = asyncTarget.replicateBatchAsync(commands, timeoutNanos);
            } else {
                future = CompletableFuture.runAsync(() -> {
                    try {
                        target.target().replicateBatch(commands, timeoutNanos);
                    } catch (IOException e) {
                        throw new java.util.concurrent.CompletionException(e);
                    }
                }, asyncExecutor);
            }
            final int index = i;
            future.whenComplete((ignored, error) -> {
                if (error == null) {
                    acked[index] = target.target().nodeId();
                    int ackedNow = ackedCount.incrementAndGet();
                    completedCount.incrementAndGet();
                    if (ackedNow >= requiredAcks && resolved.compareAndSet(false, true)) {
                        result.complete(snapshotResult(targetSnapshot.size(), acked, failed));
                    }
                    return;
                }
                Throwable cause = error instanceof java.util.concurrent.CompletionException completion && completion.getCause() != null
                        ? completion.getCause()
                        : error;
                if (!(cause instanceof java.util.concurrent.CancellationException)) {
                    failed[index] = target.target().nodeId();
                }
                int failedNow = failedCount.incrementAndGet();
                int completedNow = completedCount.incrementAndGet();
                int remaining = targetSnapshot.size() - completedNow;
                if (ackedCount.get() + remaining < requiredAcks && resolved.compareAndSet(false, true)) {
                    result.complete(snapshotResult(targetSnapshot.size(), acked, failed));
                    return;
                }
                if (failedNow == targetSnapshot.size() && resolved.compareAndSet(false, true)) {
                    result.completeExceptionally(cause);
                    return;
                }
                if (completedNow == targetSnapshot.size() && resolved.compareAndSet(false, true)) {
                    result.complete(snapshotResult(targetSnapshot.size(), acked, failed));
                }
            });
        }
        return result;
    }

    @Override
    /**
     * 返回当前 transport 对上层提交协调器建议的批次上限。
     */
    public synchronized int preferredMaxBatchSize() {
        if (transportProvider.type() != ReplicationTransportType.AERON) {
            return DEFAULT_PREFERRED_BATCH_SIZE;
        }
        int standbyCount = targets.size();
        if (standbyCount <= 1) {
            return AERON_SINGLE_STANDBY_BATCH_SIZE;
        }
        return AERON_MULTI_STANDBY_BATCH_SIZE;
    }

    @Override
    public synchronized int preferredInFlightBatches() {
        if (transportProvider.type() != ReplicationTransportType.AERON) {
            return DEFAULT_PREFERRED_IN_FLIGHT_BATCHES;
        }
        return targets.size() <= 1 ? DEFAULT_PREFERRED_IN_FLIGHT_BATCHES : AERON_MULTI_STANDBY_IN_FLIGHT_BATCHES;
    }

    @Override
    public synchronized long preferredAccumulationNanos() {
        if (transportProvider.type() != ReplicationTransportType.AERON) {
            return DEFAULT_PREFERRED_ACCUMULATION_NANOS;
        }
        return targets.size() <= 1 ? DEFAULT_PREFERRED_ACCUMULATION_NANOS : AERON_MULTI_STANDBY_ACCUMULATION_NANOS;
    }

    @Override
    public synchronized void close() throws IOException {
        IOException failure = null;
        for (TargetRef target : targets.values()) {
            try {
                target.target().close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = new IOException("failed to close replication target", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        targets.clear();
        asyncExecutor.shutdownNow();
        try {
            if (!asyncExecutor.awaitTermination(5L, TimeUnit.SECONDS)) {
                IOException timeout = new IOException("timed out waiting for replication async executor to stop");
                if (failure == null) {
                    failure = timeout;
                } else {
                    failure.addSuppressed(timeout);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            IOException interrupted = new IOException("interrupted while stopping replication async executor", e);
            if (failure == null) {
                failure = interrupted;
            } else {
                failure.addSuppressed(interrupted);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static ReplicationResult replicateSequentially(List<Command> commands,
                                                           long timeoutNanos,
                                                           List<TargetRef> targetSnapshot) throws IOException {
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        List<String> acked = new ArrayList<>(targetSnapshot.size());
        List<String> failed = new ArrayList<>();
        IOException error = null;
        for (TargetRef target : targetSnapshot) {
            try {
                target.target().replicateBatch(commands, timeoutNanos);
                acked.add(target.target().nodeId());
            } catch (IOException e) {
                failed.add(target.target().nodeId());
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
        }
        if (error != null && acked.isEmpty()) {
            throw error;
        }
        return new ReplicationResult(targetSnapshot.size(), acked.size(), List.copyOf(acked), List.copyOf(failed));
    }

    private static ReplicationResult snapshotResult(int totalTargets, String[] acked, String[] failed) {
        List<String> ackedNodeIds = Arrays.stream(acked).filter(Objects::nonNull).toList();
        List<String> failedNodeIds = Arrays.stream(failed).filter(Objects::nonNull).toList();
        return new ReplicationResult(totalTargets, ackedNodeIds.size(), ackedNodeIds, failedNodeIds);
    }

    private void closeQuietly(TargetRef existing) throws IOException {
        if (existing == null) {
            return;
        }
        try {
            existing.target().close();
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new IOException("failed to close replication target " + existing.host() + ":" + existing.port(), e);
        }
    }

    private record TargetRef(String host, int port, ClusterPeerClient target) {
        private boolean matches(DiscoveredNode node) {
            return port == node.grpcPort() && host.equals(node.host());
        }
    }
}
