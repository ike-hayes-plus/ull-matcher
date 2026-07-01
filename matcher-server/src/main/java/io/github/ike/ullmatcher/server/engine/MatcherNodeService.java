package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.CommandPool;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.HaTickResult;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterialSource;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncResult;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncSource;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.state.NodeControlStateSource;
import io.github.ike.ullmatcher.ha.state.ReplicaState;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.metrics.MatcherStats;
import io.github.ike.ullmatcher.runtime.MatchLoopSnapshot;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.telemetry.MatchingMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.MatcherNodeMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.SubmitPathMetricsSnapshot;
import io.github.ike.ullmatcher.queue.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;
import java.util.AbstractList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.LockSupport;

/**
 * 单分片节点服务编排器。
 * <p>
 * 该类把入口层、撮合引擎、WAL、快照和高可用复制链收敛成一个节点语义：
 * 控制面串行推进，提交流量独立批量推进，最后一跳仍保持单线程撮合。
 */
public final class MatcherNodeService implements Closeable, NodeControlStateSource, SnapshotMaterialSource {
    private static final Logger LOG = LoggerFactory.getLogger(MatcherNodeService.class);
    private static final int RECENT_ORDER_STATE_LIMIT = 512;
    private static final int DEFAULT_SUBMIT_BATCH_LIMIT = 256;
    private static final long DEFAULT_SUBMIT_BATCH_DELAY_MICROS = 200L;
    private static final long DEFAULT_PRIMARY_LEASE_SUBMIT_CHECK_MICROS = 1_000L;

    private final MatcherServerConfig config;
    private final AtomicLong nextSequence = new AtomicLong(1L);
    private final ExecutorService controlExecutor;
    private final MpscArrayQueue<SubmitEnvelope> submitQueue;
    private final Thread submitWorker;
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
    private final TtlCancelGuard ttlCancelGuard;
    private final OrderStateTracker orderStateTracker;
    private final SubmissionTracker submissionTracker;
    private final CommandPool commandPool;
    private final SubmissionCoordinator submissionCoordinator;
    private final SnapshotCoordinator snapshotCoordinator;
    private final ClusterRoleCoordinator clusterRoleCoordinator;
    private final EngineLifecycleManager engineLifecycleManager;

    private volatile MatcherEngine state;
    private volatile SnapshotMaterial lastSnapshot;
    private volatile boolean replicationIngressPaused;
    private volatile boolean closed;
    private volatile long nextPrimaryLeaseSubmitCheckNanos;

    public MatcherNodeService(MatcherServerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.config.validateDeploymentSafety();
        this.submitQueue = new MpscArrayQueue<>(submitQueueCapacity(config));
        this.controlExecutor = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().name("matcher-control-" + config.nodeId()).factory()
        );
        this.submitWorker = Thread.ofPlatform().name("matcher-submit-" + config.nodeId()).start(this::submitLoop);
        this.lastSnapshot = new SnapshotMaterial(config.snapshotFile(), 0L, 0L, 0L);
        this.orderStateTracker = new OrderStateTracker(RECENT_ORDER_STATE_LIMIT);
        this.submissionTracker = new SubmissionTracker(orderStateTracker);
        this.commandPool = new CommandPool(commandPoolCapacity(config));
        this.ttlCancelGuard = new TtlCancelGuard(config.ttlCancelConfig(), this::cancelForTtl, config.matcherConfig().symbolId());
        this.clusterRoleCoordinator = new ClusterRoleCoordinator(config);
        this.submissionCoordinator = new SubmissionCoordinator(config, commandPool, nextSequence, ttlCancelGuard, orderStateTracker);
        this.snapshotCoordinator = new SnapshotCoordinator(
                config.snapshotFile(),
                config.matcherConfig(),
                nextSequence,
                config.gatewayOfferTimeoutNanos(),
                ttlCancelGuard,
                orderStateTracker
        );
        this.engineLifecycleManager = new EngineLifecycleManager(
                config,
                ttlCancelGuard,
                orderStateTracker,
                snapshotCoordinator,
                clusterRoleCoordinator
        );
    }

    /**
     * 启动节点并装配初始引擎实例。
     */
    public void start() throws IOException {
        invokeControlWrite(() -> {
            if (state != null) {
                return null;
            }
            EngineLifecycleManager.EngineStartResult started = engineLifecycleManager.createEngine(config.initialRole(), new FencingToken(1L));
            state = started.engine();
            if (started.snapshotMaterial() != null) {
                lastSnapshot = started.snapshotMaterial();
            }
            nextSequence.set(state.matcher().lastSequence() + 1L);
            ttlCancelGuard.start();
            LOG.info("matcher node started nodeId={} role={}", config.nodeId(), config.initialRole().name());
            return null;
        });
    }

    public SubmitResponse submitNewOrder(long userId, long orderId, Side side, OrderType orderType, TimeInForce tif,
                                         long price, long quantity) throws IOException {
        return submitNewOrder(userId, orderId, side, orderType, tif, price, quantity, null);
    }

    public SubmitResponse submitNewOrder(long userId, long orderId, Side side, OrderType orderType, TimeInForce tif,
                                         long price, long quantity, Long ttlMillis) throws IOException {
        return invokeSubmit(new SubmitTask(new SubmissionRequest.NewOrderRequest(
                userId, orderId, side, orderType, tif, price, quantity, ttlMillis
        )));
    }

    public SubmissionTracker.SubmissionHandle submitTrackedNewOrder(long userId,
                                                                    long orderId,
                                                                    Side side,
                                                                    OrderType orderType,
                                                                    TimeInForce tif,
                                                                    long price,
                                                                    long quantity,
                                                                    Long ttlMillis,
                                                                    String idempotencyKey) throws IOException {
        SubmissionTracker.RequestFingerprint requestFingerprint = SubmissionFingerprints.newOrder(
                userId, orderId, side, orderType, tif, price, quantity, ttlMillis
        );
        SubmissionTracker.Registration registration = submissionTracker.register(
                "NEW_ORDER", idempotencyKey, userId, orderId, requestFingerprint
        );
        if (!registration.existing()) {
            enqueueTrackedSubmit(
                    new SubmitTask(new SubmissionRequest.NewOrderRequest(userId, orderId, side, orderType, tif, price, quantity, ttlMillis)),
                    registration.trackedSubmission()
            );
        }
        return submissionTracker.handle(registration.trackedSubmission());
    }

    public SubmitResponse cancelOrder(long orderId) throws IOException {
        return invokeSubmit(new SubmitTask(new SubmissionRequest.CancelOrderRequest(orderId)));
    }

    public List<SubmitResponse> submitNewOrderBatch(List<BatchNewOrderRequest> requests) throws IOException {
        Objects.requireNonNull(requests, "requests");
        if (requests.isEmpty()) {
            return List.of();
        }
        ArrayList<SubmitResponse> responses = new ArrayList<>(requests.size());
        submitNewOrderBatch(new NewOrderBatchSource() {
            @Override
            public int size() {
                return requests.size();
            }

            @Override
            public long userIdAt(int index) {
                return requests.get(index).userId();
            }

            @Override
            public long orderIdAt(int index) {
                return requests.get(index).orderId();
            }

            @Override
            public Side sideAt(int index) {
                return requests.get(index).side();
            }

            @Override
            public OrderType orderTypeAt(int index) {
                return requests.get(index).orderType();
            }

            @Override
            public TimeInForce timeInForceAt(int index) {
                return requests.get(index).timeInForce();
            }

            @Override
            public long priceAt(int index) {
                return requests.get(index).price();
            }

            @Override
            public long quantityAt(int index) {
                return requests.get(index).quantity();
            }

            @Override
            public Long ttlMillisAt(int index) {
                return requests.get(index).ttlMillis();
            }
        }, (index, result, sequence) -> responses.add(new SubmitResponse(result, sequence)));
        return responses;
    }

    public List<SubmitResponse> submitCancelOrderBatch(List<Long> orderIds) throws IOException {
        Objects.requireNonNull(orderIds, "orderIds");
        if (orderIds.isEmpty()) {
            return List.of();
        }
        ArrayList<SubmitResponse> responses = new ArrayList<>(orderIds.size());
        submitCancelOrderBatch(new CancelOrderBatchSource() {
            @Override
            public int size() {
                return orderIds.size();
            }

            @Override
            public long orderIdAt(int index) {
                return orderIds.get(index);
            }
        }, (index, result, sequence) -> responses.add(new SubmitResponse(result, sequence)));
        return responses;
    }

    /**
     * 按批量源提交新单。
     * <p>
     * 这是 binary ingress 的关键落点，目标是保住批次边界，避免在服务层过早拆批。
     */
    public void submitNewOrderBatch(NewOrderBatchSource requests, SubmitResponseConsumer consumer) throws IOException {
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(consumer, "consumer");
        if (requests.size() == 0) {
            return;
        }
        SubmitTask[] tasks = new SubmitTask[requests.size()];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new SubmitTask(new SubmissionRequest.NewOrderRequest(
                    requests.userIdAt(i),
                    requests.orderIdAt(i),
                    requests.sideAt(i),
                    requests.orderTypeAt(i),
                    requests.timeInForceAt(i),
                    requests.priceAt(i),
                    requests.quantityAt(i),
                    requests.ttlMillisAt(i)
            ), false);
        }
        enqueueSubmitEnvelope(SubmitEnvelope.batch(tasks, tasks.length));
        for (int i = 0; i < tasks.length; i++) {
            tasks[i].awaitCompletion();
            consumer.accept(i, tasks[i].result(), tasks[i].sequence());
        }
    }

    /**
     * 按批量源提交撤单。
     */
    public void submitCancelOrderBatch(CancelOrderBatchSource orderIds, SubmitResponseConsumer consumer) throws IOException {
        Objects.requireNonNull(orderIds, "orderIds");
        Objects.requireNonNull(consumer, "consumer");
        if (orderIds.size() == 0) {
            return;
        }
        SubmitTask[] tasks = new SubmitTask[orderIds.size()];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new SubmitTask(new SubmissionRequest.CancelOrderRequest(orderIds.orderIdAt(i)), false);
        }
        enqueueSubmitEnvelope(SubmitEnvelope.batch(tasks, tasks.length));
        for (int i = 0; i < tasks.length; i++) {
            tasks[i].awaitCompletion();
            consumer.accept(i, tasks[i].result(), tasks[i].sequence());
        }
    }

    public SubmissionTracker.SubmissionHandle submitTrackedCancelOrder(long orderId, String idempotencyKey) throws IOException {
        SubmissionTracker.Registration registration = submissionTracker.register(
                "CANCEL_ORDER", idempotencyKey, null, orderId, SubmissionFingerprints.cancel(orderId)
        );
        if (!registration.existing()) {
            enqueueTrackedSubmit(new SubmitTask(new SubmissionRequest.CancelOrderRequest(orderId)), registration.trackedSubmission());
        }
        return submissionTracker.handle(registration.trackedSubmission());
    }

    public SnapshotMaterial createSnapshot() throws IOException {
        return invokeControlWrite(this::createSnapshotInternal);
    }

    @Override
    public SnapshotMaterial latestSnapshot() throws IOException {
        return invokeControlWrite(() -> {
            if (Files.exists(config.snapshotFile()) && lastSnapshot.file().equals(config.snapshotFile())) {
                return lastSnapshot;
            }
            return createSnapshotInternal();
        });
    }

    public SnapshotSyncResult installSnapshotFrom(SnapshotSyncSource source, long timeoutNanos) throws IOException {
        Objects.requireNonNull(source, "source");
        return invokeControlWrite(() -> {
            replicationIngressPaused = true;
            try {
                Path tmp = config.snapshotFile().resolveSibling(config.snapshotFile().getFileName() + ".sync");
                SnapshotSyncResult result = source.downloadLatestSnapshot(tmp, timeoutNanos);
                Files.move(result.file(), config.snapshotFile(), java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                restartFromSnapshot();
                lastSnapshot = new SnapshotMaterial(config.snapshotFile(), result.lastSequence(), result.lastTradeId(), result.liveOrderCount());
                return new SnapshotSyncResult(config.snapshotFile(), result.bytesWritten(), result.lastSequence(), result.lastTradeId(), result.liveOrderCount());
            } finally {
                replicationIngressPaused = false;
            }
        });
    }

    public SnapshotSyncResult rejoinFencedFromSnapshot(SnapshotSyncSource source, long timeoutNanos) throws IOException {
        Objects.requireNonNull(source, "source");
        return invokeControlWrite(() -> {
            MatcherEngine current = requireStarted();
            if (current.runtime().role() != HaRole.FENCED) {
                throw new IllegalStateException("only fenced runtime can rejoin from authoritative snapshot");
            }
            replicationIngressPaused = true;
            try {
                Path tmp = config.snapshotFile().resolveSibling(config.snapshotFile().getFileName() + ".rejoin");
                SnapshotSyncResult result = source.downloadLatestSnapshot(tmp, timeoutNanos);
                FencingToken token = current.runtime().fencingToken();
                current.close();
                state = null;
                quarantineWalDirectory();
                Files.move(result.file(), config.snapshotFile(), java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                orderStateTracker.reset();
                ttlCancelGuard.resetTrackedState();
                EngineLifecycleManager.EngineStartResult restarted = engineLifecycleManager.createEngine(HaRole.STANDBY, token);
                state = restarted.engine();
                lastSnapshot = new SnapshotMaterial(config.snapshotFile(), result.lastSequence(), result.lastTradeId(), result.liveOrderCount());
                nextSequence.set(state.matcher().lastSequence() + 1L);
                LOG.warn("fenced node rejoined as standby from authoritative snapshot nodeId={} source={} lastSequence={}",
                        config.nodeId(), source.nodeId(), result.lastSequence());
                return new SnapshotSyncResult(config.snapshotFile(), result.bytesWritten(), result.lastSequence(),
                        result.lastTradeId(), result.liveOrderCount());
            } finally {
                replicationIngressPaused = false;
            }
        });
    }

    public NodeControlState health() {
        return currentState();
    }

    @Override
    public NodeControlState currentState() {
        long snapshotSequence = lastSnapshot.file().equals(config.snapshotFile()) ? lastSnapshot.lastSequence() : 0L;
        return clusterRoleCoordinator.currentState(state, replicationIngressPaused, snapshotSequence);
    }

    public StandbySyncService standbySyncService() {
        return clusterRoleCoordinator.standbySyncService(requireStarted(), replicationIngressPaused);
    }

    public void beginCatchUp() throws IOException {
        invokeControlWrite(() -> {
            clusterRoleCoordinator.beginCatchUp(requireStarted());
            return null;
        });
    }

    public void configureReplication(CommandReplicator replicator, ReplicationMode mode, long timeoutNanos) throws IOException {
        Objects.requireNonNull(replicator, "replicator");
        Objects.requireNonNull(mode, "mode");
        invokeControlWrite(() -> {
            clusterRoleCoordinator.configureReplication(replicator, mode, timeoutNanos, requireStarted());
            return null;
        });
    }

    public void clearReplication() throws IOException {
        invokeControlWrite(() -> {
            clusterRoleCoordinator.clearReplication(requireStarted());
            return null;
        });
    }

    public HaTickResult tickHa(LeaseStore leaseStore,
                               FailoverPolicy failoverPolicy,
                               long leaseTtlNanos,
                               ReplicaState observedPrimary,
                               List<ReplicaState> standbys,
                               long nowNanos) throws IOException {
        return invokeControlWrite(() -> {
            return clusterRoleCoordinator.tickHa(
                    requireStarted(),
                    leaseStore,
                    failoverPolicy,
                    leaseTtlNanos,
                    observedPrimary,
                    standbys,
                    nowNanos
            );
        });
    }

    public boolean fencePrimaryAfterControlPlaneFailure(String reason) throws IOException {
        Objects.requireNonNull(reason, "reason");
        return invokeControlWrite(() -> {
            MatcherEngine current = requireStarted();
            if (current.runtime().role() != HaRole.PRIMARY) {
                return false;
            }
            current.runtime().fence();
            clusterRoleCoordinator.replicationCoordinator().clear();
            LOG.warn("primary fenced after control-plane failure nodeId={} token={} reason={}",
                    config.nodeId(), current.runtime().fencingToken().epoch(), reason);
            return true;
        });
    }

    public int liveOrderCount() {
        return Math.toIntExact(requireStarted().matcher().liveOrderCount());
    }

    public MatcherNodeMetricsSnapshot metricsSnapshot() {
        MatcherEngine current = requireStarted();
        MatchLoopSnapshot loopSnapshot = current.loop().snapshot();
        MatcherStats matcherStats = current.matcher().stats();
        long walSegments;
        try {
            walSegments = current.wal().segments().size();
        } catch (IOException e) {
            walSegments = -1L;
        }
        return new MatcherNodeMetricsSnapshot(
                currentState(),
                loopSnapshot,
                current.matcher().liveOrderCount(),
                current.matcher().lastTradeId(),
                new MatchingMetricsSnapshot(
                        matcherStats.tradeCount(),
                        matcherStats.orderEventCount(),
                        matcherStats.rejectedCommandCount(),
                        matcherStats.capacityRejectedCommandCount()
                ),
                walSegments,
                current.wal().currentSegmentBytes(),
                lastSnapshot,
                ttlCancelGuard.snapshot(),
                new SubmitPathMetricsSnapshot(
                        submitQueue.size(),
                        submitQueue.capacity(),
                        current.ring().size(),
                        current.ring().remainingCapacity(),
                        current.gateway().acceptedCount(),
                        current.gateway().walAppendCount(),
                        current.gateway().walForceCount(),
                        current.gateway().failedBeforeWalCount(),
                        current.gateway().failedAfterWalCount(),
                        current.gateway().lastSubmitResult().name()
                ),
                submissionTracker.metricsSnapshot(),
                clusterRoleCoordinator.replicationCoordinator().metricsSnapshot(),
                current.standbySyncService().metricsSnapshot()
        );
    }

    public OrderStateView orderState(long orderId) {
        return orderStateTracker.find(orderId);
    }

    public List<OrderStateView> recentOrderStates(int limit) {
        return orderStateTracker.recent(limit);
    }

    public SubmissionView submission(String submissionId) {
        return submissionTracker.findBySubmissionId(submissionId);
    }

    public SubmissionView submissionByIdempotencyKey(String idempotencyKey) {
        return submissionTracker.findByIdempotencyKey(idempotencyKey);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException closeError = null;
        Future<Void> closeFuture;
        try {
            closeFuture = controlExecutor.submit(() -> {
                lifecycleLock.writeLock().lock();
                try {
                if (state != null) {
                    state.close();
                    state = null;
                }
                } finally {
                    lifecycleLock.writeLock().unlock();
                }
                return null;
            });
            closeFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeError = new IOException("interrupted while closing matcher node service", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            closeError = cause instanceof IOException io
                    ? io
                    : new IOException("failed to close matcher node service", cause);
        } finally {
            controlExecutor.shutdownNow();
            submitWorker.interrupt();
        }
        try {
            clusterRoleCoordinator.replicationCoordinator().close();
        } catch (IOException e) {
            if (closeError == null) {
                closeError = e;
            } else {
                closeError.addSuppressed(e);
            }
        }
        try {
            ttlCancelGuard.close();
        } catch (IOException e) {
            if (closeError == null) {
                closeError = e;
            } else {
                closeError.addSuppressed(e);
            }
        }
        if (closeError != null) {
            throw closeError;
        }
    }

    private void restartFromSnapshot() throws IOException {
        EngineLifecycleManager.RestartResult restarted = engineLifecycleManager.restartFromSnapshot(requireStarted());
        state = restarted.engine();
        if (restarted.snapshotMaterial() != null) {
            lastSnapshot = restarted.snapshotMaterial();
        }
        nextSequence.set(state.matcher().lastSequence() + 1L);
        LOG.info("matcher engine restarted from snapshot nodeId={} role={}", config.nodeId(), restarted.role().name());
    }

    private void quarantineWalDirectory() throws IOException {
        Path walDirectory = config.walDirectory();
        if (!Files.exists(walDirectory)) {
            return;
        }
        Path quarantine = walDirectory.resolveSibling(walDirectory.getFileName() + ".fenced."
                + System.currentTimeMillis() + "." + System.nanoTime());
        Files.move(walDirectory, quarantine);
        LOG.warn("quarantined fenced local WAL before standby rejoin nodeId={} from={} to={}",
                config.nodeId(), walDirectory, quarantine);
    }

    private MatcherEngine requireStarted() {
        MatcherEngine current = state;
        if (current == null) {
            throw new IllegalStateException("matcher node service is not started");
        }
        return current;
    }

    public record SubmitResponse(SubmitResult result, long sequence) {}

    public record BatchNewOrderRequest(long userId,
                                       long orderId,
                                       Side side,
                                       OrderType orderType,
                                       TimeInForce timeInForce,
                                       long price,
                                       long quantity,
                                       Long ttlMillis) {}

    public interface NewOrderBatchSource {
        int size();
        long userIdAt(int index);
        long orderIdAt(int index);
        Side sideAt(int index);
        OrderType orderTypeAt(int index);
        TimeInForce timeInForceAt(int index);
        long priceAt(int index);
        long quantityAt(int index);
        Long ttlMillisAt(int index);
    }

    public interface CancelOrderBatchSource {
        int size();
        long orderIdAt(int index);
    }

    @FunctionalInterface
    public interface SubmitResponseConsumer {
        void accept(int index, SubmitResult result, long sequence) throws IOException;
    }

    private SubmitResult cancelForTtl(long orderId) throws IOException {
        return cancelOrder(orderId).result();
    }

    private SnapshotMaterial createSnapshotInternal() throws IOException {
        lastSnapshot = snapshotCoordinator.createSnapshot(requireStarted());
        LOG.info("snapshot created nodeId={} file={} lastSequence={}",
                config.nodeId(), lastSnapshot.file(), lastSnapshot.lastSequence());
        return lastSnapshot;
    }

    private <T> T invokeControlWrite(Callable<T> task) throws IOException {
        return invokeWithExecutor(controlExecutor, true, task);
    }

    private SubmitResponse invokeSubmit(SubmitTask task) throws IOException {
        enqueueTrackedSubmit(task, null);
        return task.awaitResult();
    }

    private void enqueueTrackedSubmit(SubmitTask task, SubmissionTracker.TrackedSubmission trackedSubmission) throws IOException {
        if (closed) {
            throw new IllegalStateException("matcher node service is closed");
        }
        task.attachSubmission(trackedSubmission);
        enqueueSubmitEnvelope(SubmitEnvelope.single(task));
    }

    private void enqueueSubmitEnvelope(SubmitEnvelope envelope) throws IOException {
        if (closed) {
            throw new IllegalStateException("matcher node service is closed");
        }
        guardPrimaryLeaseForSubmit();
        long deadline = System.nanoTime() + submitEnqueueTimeoutNanos(config);
        int spins = 0;
        while (!submitQueue.offer(envelope)) {
            if (closed) {
                throw new IllegalStateException("matcher node service is closed");
            }
            guardPrimaryLeaseForSubmit();
            if (System.nanoTime() >= deadline) {
                throw new IOException("timed out while enqueueing matcher submit task");
            }
            if (++spins >= 256) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(50));
                spins = 0;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private void guardPrimaryLeaseForSubmit() throws IOException {
        if (config.clusterConfig() == null) {
            return;
        }
        MatcherEngine current = state;
        if (current == null || current.runtime().role() != HaRole.PRIMARY) {
            return;
        }
        long nowNanos = System.nanoTime();
        if (nowNanos < nextPrimaryLeaseSubmitCheckNanos) {
            return;
        }
        synchronized (this) {
            nowNanos = System.nanoTime();
            if (nowNanos < nextPrimaryLeaseSubmitCheckNanos) {
                return;
            }
            nextPrimaryLeaseSubmitCheckNanos = nowNanos + primaryLeaseSubmitCheckIntervalNanos();
        }
        if (config.clusterConfig().leaseStore().isHeldBy(config.nodeId(), current.runtime().fencingToken(), nowNanos)) {
            return;
        }

        boolean leaseLost = false;
        lifecycleLock.writeLock().lock();
        try {
            current = state;
            nowNanos = System.nanoTime();
            if (current != null
                    && current.runtime().role() == HaRole.PRIMARY
                    && !config.clusterConfig().leaseStore().isHeldBy(config.nodeId(), current.runtime().fencingToken(), nowNanos)) {
                leaseLost = true;
                current.runtime().fence();
                clusterRoleCoordinator.replicationCoordinator().clear();
                LOG.warn("primary submit fenced after lease loss nodeId={} token={}",
                        config.nodeId(), current.runtime().fencingToken().epoch());
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        if (leaseLost) {
            throw new IOException("primary lease is not held by local node; rejecting write");
        }
    }

    private static long primaryLeaseSubmitCheckIntervalNanos() {
        long configuredMicros = Long.getLong(
                "matcher.primaryLeaseSubmitCheckMicros",
                DEFAULT_PRIMARY_LEASE_SUBMIT_CHECK_MICROS
        );
        return TimeUnit.MICROSECONDS.toNanos(Math.max(0L, configuredMicros));
    }

    private <T> T invokeWithExecutor(ExecutorService executor, boolean writeLock, Callable<T> task) throws IOException {
        if (closed) {
            throw new IllegalStateException("matcher node service is closed");
        }
        final Future<T> future;
        try {
            future = executor.submit(() -> {
                var lock = writeLock ? lifecycleLock.writeLock() : lifecycleLock.readLock();
                lock.lock();
                try {
                    return task.call();
                } finally {
                    lock.unlock();
                }
            });
        } catch (RejectedExecutionException e) {
            throw new IllegalStateException((writeLock ? "matcher control thread" : "matcher submit thread") + " is not accepting work", e);
        }
        return awaitTaskResult(future);
    }

    private <T> T awaitTaskResult(Future<T> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for matcher control task", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException("matcher control task failed", cause);
        }
    }

    private void submitLoop() {
        int batchLimit = submitBatchLimit(config);
        long batchDelayNanos = submitBatchDelayNanos(config);
        ArrayList<SubmitTask> batch = new ArrayList<>(batchLimit);
        SubmitTaskRequestView requests = new SubmitTaskRequestView(batch);
        SubmissionCoordinator.BatchSubmitContext batchContext = new SubmissionCoordinator.BatchSubmitContext(batchLimit);
        SubmissionRequest.PreparedSubmission[] replicationPrepared = new SubmissionRequest.PreparedSubmission[batchLimit];
        SubmissionTracker.TrackedSubmission[] replicationSubmissions = new SubmissionTracker.TrackedSubmission[batchLimit];
        long idleParkNanos = TimeUnit.MICROSECONDS.toNanos(100);
        SubmitEnvelope deferred = null;
        while (!closed || submitQueue.size() > 0) {
            SubmitEnvelope first = deferred != null ? deferred : submitQueue.poll();
            deferred = null;
            if (first == null) {
                LockSupport.parkNanos(idleParkNanos);
                continue;
            }
            batch.clear();
            appendEnvelopeTasks(batch, first);
            while (batch.size() < batchLimit) {
                SubmitEnvelope next = submitQueue.poll();
                if (next == null) {
                    break;
                }
                if (batch.size() + next.tasks().size() > batchLimit) {
                    deferred = next;
                    break;
                }
                appendEnvelopeTasks(batch, next);
            }
            if (batchDelayNanos > 0L && batch.size() < batchLimit) {
                long deadline = System.nanoTime() + batchDelayNanos;
                while (batch.size() < batchLimit) {
                    SubmitEnvelope next = submitQueue.poll();
                    if (next != null) {
                        if (batch.size() + next.tasks().size() > batchLimit) {
                            deferred = next;
                            break;
                        }
                        appendEnvelopeTasks(batch, next);
                        continue;
                    }
                    if (System.nanoTime() >= deadline) {
                        break;
                    }
                    Thread.onSpinWait();
                }
            }
                lifecycleLock.readLock().lock();
                int replicationCount = 0;
                try {
                    SubmissionCoordinator.BatchSubmitOutcome batchResult = submissionCoordinator.submitBatch(requireStarted(), requests, batchContext);
                    SubmitResult result = batchContext.result;
                    for (int i = 0; i < batch.size(); i++) {
                        SubmissionTracker.TrackedSubmission trackedSubmission = batch.get(i).trackedSubmission();
                        SubmissionRequest.PreparedSubmission prepared = i < batchResult.preparedCount()
                                ? batchContext.prepared[i]
                                : null;
                        long sequence = prepared == null ? 0L : prepared.sequence();
                        if (trackedSubmission != null) {
                            trackedSubmission.markLocalOutcome(
                                    sequence,
                                    result,
                                    System.currentTimeMillis(),
                                    result == io.github.ike.ullmatcher.hft.SubmitResult.ACCEPTED && batchResult.replicationRequired(),
                                    batchResult.requiredAcks(),
                                    batchResult.totalTargets()
                            );
                        }
                        if (prepared != null && result == io.github.ike.ullmatcher.hft.SubmitResult.ACCEPTED) {
                            replicationPrepared[replicationCount] = prepared;
                            replicationSubmissions[replicationCount] = trackedSubmission;
                            replicationCount++;
                        }
                        batch.get(i).complete(result, sequence);
                    }
                    if (replicationCount > 0) {
                        clusterRoleCoordinator.replicationCoordinator().onLocalAcceptedBatch(replicationPrepared, replicationSubmissions, replicationCount);
                    }
                } catch (Throwable error) {
                    for (SubmitTask task : batch) {
                        if (task.trackedSubmission() != null) {
                            task.trackedSubmission().markClosedFailure(error.getMessage(), System.currentTimeMillis());
                        }
                        task.fail(error);
                    }
                } finally {
                    if (replicationCount > 0) {
                        clearReplicationBatch(replicationPrepared, replicationSubmissions, replicationCount);
                    }
                    lifecycleLock.readLock().unlock();
                }
        }
        SubmitEnvelope remaining;
        while ((remaining = submitQueue.poll()) != null) {
            for (SubmitTask task : remaining.tasks()) {
                task.fail(new IllegalStateException("matcher node service is closed"));
            }
        }
    }

    private static void appendEnvelopeTasks(List<SubmitTask> batch, SubmitEnvelope envelope) {
        batch.addAll(envelope.tasks());
    }

    private static void clearReplicationBatch(SubmissionRequest.PreparedSubmission[] prepared,
                                              SubmissionTracker.TrackedSubmission[] submissions,
                                              int size) {
        for (int i = 0; i < size; i++) {
            prepared[i] = null;
            submissions[i] = null;
        }
    }

    private static int submitQueueCapacity(MatcherServerConfig config) {
        int minimum = Math.max(config.ringCapacity(), config.httpSubmitEndpointMaxConcurrentRequests() * 4);
        int highest = Integer.highestOneBit(Math.max(2, minimum - 1));
        return highest << 1;
    }

    private static int commandPoolCapacity(MatcherServerConfig config) {
        int minimum = Math.max(
                config.ringCapacity() * 4,
                submitQueueCapacity(config) + config.ringCapacity() + submitBatchLimit(config) * 2
        );
        int highest = Integer.highestOneBit(Math.max(2, minimum - 1));
        return highest << 1;
    }

    private static int submitBatchLimit(MatcherServerConfig config) {
        int desired = Math.max(
                Math.max(1, config.walForceBatchSize()),
                Math.max(1, Math.max(config.binaryIngressMaxBatchSize(), DEFAULT_SUBMIT_BATCH_LIMIT))
        );
        return Math.min(config.ringCapacity(), desired);
    }

    private static long submitBatchDelayNanos(MatcherServerConfig config) {
        long configuredMicros = config.walForceMaxDelayMicros();
        long effectiveMicros = configuredMicros > 0L ? configuredMicros : DEFAULT_SUBMIT_BATCH_DELAY_MICROS;
        return TimeUnit.MICROSECONDS.toNanos(effectiveMicros);
    }

    private static long submitEnqueueTimeoutNanos(MatcherServerConfig config) {
        return Math.max(
                config.gatewayOfferTimeoutNanos(),
                TimeUnit.MILLISECONDS.toNanos(config.httpWriteTimeoutMillis())
        );
    }

    private static final class SubmitTask {
        private final SubmissionRequest request;
        private final boolean responseRequired;
        private SubmissionTracker.TrackedSubmission trackedSubmission;
        private volatile boolean done;
        private SubmitResult result;
        private long sequence;
        private volatile SubmitResponse response;
        private volatile Throwable failure;
        private volatile Thread waiter;

        private SubmitTask(SubmissionRequest request) {
            this(request, true);
        }

        private SubmitTask(SubmissionRequest request, boolean responseRequired) {
            this.request = Objects.requireNonNull(request, "request");
            this.responseRequired = responseRequired;
        }

        private SubmissionRequest request() {
            return request;
        }

        private void attachSubmission(SubmissionTracker.TrackedSubmission trackedSubmission) {
            this.trackedSubmission = trackedSubmission;
        }

        private SubmissionTracker.TrackedSubmission trackedSubmission() {
            return trackedSubmission;
        }

        private void complete(SubmitResult result, long sequence) {
            this.result = result;
            this.sequence = sequence;
            if (responseRequired) {
                response = new SubmitResponse(result, sequence);
            }
            done = true;
            Thread parked = waiter;
            if (parked != null) {
                LockSupport.unpark(parked);
            }
        }

        private void fail(Throwable error) {
            failure = error;
            done = true;
            Thread parked = waiter;
            if (parked != null) {
                LockSupport.unpark(parked);
            }
        }

        private SubmitResult result() {
            return result;
        }

        private long sequence() {
            return sequence;
        }

        private SubmitResponse awaitResult() throws IOException {
            awaitCompletion();
            SubmitResponse completedResponse = response;
            return completedResponse != null ? completedResponse : new SubmitResponse(result, sequence);
        }

        private void awaitCompletion() throws IOException {
            if (!done) {
                waiter = Thread.currentThread();
                while (!done) {
                    LockSupport.park(this);
                    if (Thread.interrupted()) {
                        waiter = null;
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted while waiting for matcher submit task", new InterruptedException());
                    }
                }
                waiter = null;
            }
            if (failure != null) {
                if (failure instanceof IOException io) {
                    throw io;
                }
                if (failure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (failure instanceof Error error) {
                    throw error;
                }
                throw new IOException("matcher submit task failed", failure);
            }
        }
    }

    private static final class SubmitEnvelope {
        private final List<SubmitTask> tasks;

        private SubmitEnvelope(List<SubmitTask> tasks) {
            this.tasks = Objects.requireNonNull(tasks, "tasks");
        }

        private static SubmitEnvelope single(SubmitTask task) {
            return new SubmitEnvelope(List.of(task));
        }

        private static SubmitEnvelope batch(SubmitTask[] tasks, int size) {
            return new SubmitEnvelope(new SubmitTaskArrayView(tasks, size));
        }

        private static SubmitEnvelope batch(List<SubmitTask> tasks) {
            return new SubmitEnvelope(tasks);
        }

        private List<SubmitTask> tasks() {
            return tasks;
        }
    }

    private static final class SubmitTaskArrayView extends AbstractList<SubmitTask> {
        private final SubmitTask[] tasks;
        private final int size;

        private SubmitTaskArrayView(SubmitTask[] tasks, int size) {
            this.tasks = Objects.requireNonNull(tasks, "tasks");
            this.size = size;
        }

        @Override
        public SubmitTask get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(index);
            }
            return tasks[index];
        }

        @Override
        public int size() {
            return size;
        }
    }

    private static final class SubmitTaskRequestView extends AbstractList<SubmissionRequest> {
        private final List<SubmitTask> batch;

        private SubmitTaskRequestView(List<SubmitTask> batch) {
            this.batch = batch;
        }

        @Override
        public SubmissionRequest get(int index) {
            return batch.get(index).request();
        }

        @Override
        public int size() {
            return batch.size();
        }
    }

}
