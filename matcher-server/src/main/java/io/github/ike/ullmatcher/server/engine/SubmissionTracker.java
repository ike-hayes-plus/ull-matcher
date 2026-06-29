package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.server.telemetry.SubmissionMetricsSnapshot;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * 提交幂等、状态跟踪与结果查询存储。
 */
final class SubmissionTracker {
    private static final int DEFAULT_MAX_TRACKED_SUBMISSIONS = 65_536;

    private final AtomicLong nextSubmissionId = new AtomicLong(1L);
    private final ConcurrentHashMap<String, TrackedSubmission> bySubmissionId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TrackedSubmission> byIdempotencyKey = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> finalizedSubmissionIds = new ConcurrentLinkedQueue<>();
    private final Object evictionLock = new Object();
    private final Clock clock;
    private final OrderStateTracker orderStateTracker;
    private final int maxTrackedSubmissions;
    private final LongAdder trackedCount = new LongAdder();
    private final LongAdder pendingCount = new LongAdder();
    private final LongAdder committedCount = new LongAdder();
    private final LongAdder failedCount = new LongAdder();
    private final LongAdder retryingCount = new LongAdder();

    SubmissionTracker(OrderStateTracker orderStateTracker) {
        this(orderStateTracker, Clock.systemUTC(), DEFAULT_MAX_TRACKED_SUBMISSIONS);
    }

    SubmissionTracker(OrderStateTracker orderStateTracker, Clock clock, int maxTrackedSubmissions) {
        if (maxTrackedSubmissions <= 0) {
            throw new IllegalArgumentException("maxTrackedSubmissions must be positive");
        }
        this.orderStateTracker = Objects.requireNonNull(orderStateTracker, "orderStateTracker");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.maxTrackedSubmissions = maxTrackedSubmissions;
    }

    /**
     * 注册一条需要幂等查询和结果跟踪的提交。
     * <p>
     * 热路径只维护最必要的状态；对外查询视图在读取时再与订单状态拼接。
     */
    Registration register(String operationType, String idempotencyKey, Long userId, long orderId) {
        return register(operationType, idempotencyKey, userId, orderId, requestFingerprint(operationType, userId, orderId));
    }

    Registration register(String operationType, String idempotencyKey, Long userId, long orderId, long requestFingerprint) {
        return register(operationType, idempotencyKey, userId, orderId, RequestFingerprint.generic(requestFingerprint));
    }

    Registration register(String operationType, String idempotencyKey, Long userId, long orderId, RequestFingerprint requestFingerprint) {
        Objects.requireNonNull(operationType, "operationType");
        Objects.requireNonNull(requestFingerprint, "requestFingerprint");
        String normalizedKey = normalizeIdempotencyKey(idempotencyKey);
        TrackedSubmission existing = byIdempotencyKey.get(normalizedKey);
        if (existing != null) {
            existing.verifySameRequest(operationType, userId, orderId, requestFingerprint);
            return new Registration(existing, true);
        }
        long now = clock.millis();
        TrackedSubmission created = new TrackedSubmission(
                this,
                Long.toUnsignedString(nextSubmissionId.getAndIncrement(), 36),
                normalizedKey,
                operationType,
                userId,
                orderId,
                requestFingerprint,
                now
        );
        TrackedSubmission raced = byIdempotencyKey.putIfAbsent(normalizedKey, created);
        if (raced != null) {
            raced.verifySameRequest(operationType, userId, orderId, requestFingerprint);
            return new Registration(raced, true);
        }
        bySubmissionId.put(created.submissionId(), created);
        trackedCount.increment();
        pendingCount.increment();
        if (trackedCount.sum() > maxTrackedSubmissions) {
            evictFinalizedIfNeeded();
            if (trackedCount.sum() > maxTrackedSubmissions) {
                bySubmissionId.remove(created.submissionId(), created);
                byIdempotencyKey.remove(normalizedKey, created);
                trackedCount.decrement();
                pendingCount.decrement();
                throw new IllegalStateException("too many tracked submissions");
            }
        }
        return new Registration(created, false);
    }

    /**
     * 按 submissionId 查询提交视图。
     */
    SubmissionView findBySubmissionId(String submissionId) {
        TrackedSubmission tracked = bySubmissionId.get(submissionId);
        return tracked == null ? null : tracked.snapshot(orderStateTracker.find(tracked.orderId()));
    }

    /**
     * 按幂等键查询提交视图。
     */
    SubmissionView findByIdempotencyKey(String idempotencyKey) {
        TrackedSubmission tracked = byIdempotencyKey.get(normalizeIdempotencyKey(idempotencyKey));
        return tracked == null ? null : tracked.snapshot(orderStateTracker.find(tracked.orderId()));
    }

    SubmissionHandle handle(TrackedSubmission tracked) {
        return new SubmissionHandle(tracked, orderStateTracker);
    }

    SubmissionMetricsSnapshot metricsSnapshot() {
        return new SubmissionMetricsSnapshot(
                trackedCount.sum(),
                pendingCount.sum(),
                committedCount.sum(),
                failedCount.sum(),
                retryingCount.sum()
        );
    }

    record Registration(TrackedSubmission trackedSubmission, boolean existing) {
    }

    public static final class SubmissionHandle {
        private final TrackedSubmission tracked;
        private final OrderStateTracker orderStateTracker;

        private SubmissionHandle(TrackedSubmission tracked, OrderStateTracker orderStateTracker) {
            this.tracked = Objects.requireNonNull(tracked, "tracked");
            this.orderStateTracker = Objects.requireNonNull(orderStateTracker, "orderStateTracker");
        }

        public String submissionId() {
            return tracked.submissionId();
        }

        public String idempotencyKey() {
            return tracked.idempotencyKey();
        }

        public SubmissionView snapshot() {
            return tracked.snapshot(orderStateTracker.find(tracked.orderId()));
        }

        public SubmissionReceipt awaitLocalReceipt(long timeoutMillis) throws IOException {
            return tracked.awaitLocalReceipt(timeoutMillis);
        }

        public SubmissionView awaitCommitted(long timeoutMillis) throws IOException {
            return tracked.awaitCommitted(timeoutMillis, orderStateTracker);
        }
    }

    /**
     * 单条提交的内部状态机。
     * <p>
     * 它把本地受理、复制重试、复制确认和最终失败收敛成一个可查询对象。
     */
    static final class TrackedSubmission {
        private final SubmissionTracker owner;
        private final String submissionId;
        private final String idempotencyKey;
        private final String operationType;
        private final Long userId;
        private final long orderId;
        private final RequestFingerprint requestFingerprint;
        private final long createdAtEpochMillis;
        private final OutcomeSignal localOutcome = new OutcomeSignal();
        private final OutcomeSignal committedOutcome = new OutcomeSignal();

        private volatile long sequence;
        private volatile SubmissionPhase phase = SubmissionPhase.RECEIVED;
        private volatile SubmitResult localResult;
        private volatile boolean localDurable;
        private volatile boolean replicationRequired;
        private volatile boolean replicationCommitted;
        private volatile int totalTargets;
        private volatile int requiredAcks;
        private volatile int ackedTargets;
        private volatile List<String> ackedNodeIds = List.of();
        private volatile List<String> failedNodeIds = List.of();
        private volatile long retryCount;
        private volatile String lastError = "";
        private volatile long updatedAtEpochMillis;
        private volatile boolean finalized;

        private TrackedSubmission(SubmissionTracker owner,
                                  String submissionId,
                                  String idempotencyKey,
                                  String operationType,
                                  Long userId,
                                  long orderId,
                                  RequestFingerprint requestFingerprint,
                                  long createdAtEpochMillis) {
            this.owner = owner;
            this.submissionId = submissionId;
            this.idempotencyKey = idempotencyKey;
            this.operationType = operationType;
            this.userId = userId;
            this.orderId = orderId;
            this.requestFingerprint = requestFingerprint;
            this.createdAtEpochMillis = createdAtEpochMillis;
            this.updatedAtEpochMillis = createdAtEpochMillis;
        }

        String submissionId() {
            return submissionId;
        }

        String idempotencyKey() {
            return idempotencyKey;
        }

        long orderId() {
            return orderId;
        }

        void verifySameRequest(String operationType, Long userId, long orderId, RequestFingerprint requestFingerprint) {
            if (this.orderId != orderId || !this.requestFingerprint.equals(requestFingerprint) ||
                    !this.operationType.equals(operationType) || !Objects.equals(this.userId, userId)) {
                throw new IllegalStateException("idempotency key reused with different request");
            }
        }

        synchronized void markLocalOutcome(long sequence,
                                           SubmitResult localResult,
                                           long nowEpochMillis,
                                           boolean replicationRequired,
                                           int requiredAcks,
                                           int totalTargets) {
            if (phase != SubmissionPhase.RECEIVED) {
                return;
            }
            this.sequence = sequence;
            this.localResult = localResult;
            this.localDurable = localResult.walAppended();
            this.replicationRequired = replicationRequired;
            this.requiredAcks = requiredAcks;
            this.totalTargets = totalTargets;
            this.updatedAtEpochMillis = nowEpochMillis;
            if (localResult != SubmitResult.ACCEPTED) {
                this.phase = SubmissionPhase.FAILED;
                this.finalized = true;
                owner.pendingCount.decrement();
                owner.failedCount.increment();
                localOutcome.complete();
                committedOutcome.complete();
                owner.onFinalized(this);
                return;
            }
            if (!replicationRequired) {
                this.phase = SubmissionPhase.COMMITTED;
                this.replicationCommitted = true;
                this.finalized = true;
                owner.pendingCount.decrement();
                owner.committedCount.increment();
                localOutcome.complete();
                committedOutcome.complete();
                owner.onFinalized(this);
                return;
            }
            this.phase = SubmissionPhase.REPLICATION_PENDING;
            localOutcome.complete();
        }

        synchronized void markReplicationRetry(IOException error, long nowEpochMillis) {
            if (finalized || phase == SubmissionPhase.RECEIVED) {
                return;
            }
            if (retryCount == 0L) {
                owner.retryingCount.increment();
            }
            retryCount++;
            updatedAtEpochMillis = nowEpochMillis;
            lastError = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
        }

        synchronized void markReplicationCommitted(ReplicationResult result, ReplicationMode mode, long nowEpochMillis) {
            if (finalized) {
                return;
            }
            totalTargets = result.totalTargets();
            requiredAcks = mode.requiredAcks(result.totalTargets());
            ackedTargets = result.ackedTargets();
            ackedNodeIds = result.ackedNodeIds();
            failedNodeIds = result.failedNodeIds();
            updatedAtEpochMillis = nowEpochMillis;
            replicationCommitted = true;
            phase = SubmissionPhase.COMMITTED;
            finalized = true;
            owner.pendingCount.decrement();
            owner.committedCount.increment();
            if (retryCount > 0L) {
                owner.retryingCount.decrement();
            }
            committedOutcome.complete();
            owner.onFinalized(this);
        }

        synchronized void markReplicationObservation(ReplicationResult result, ReplicationMode mode, long nowEpochMillis) {
            if (finalized) {
                return;
            }
            totalTargets = result.totalTargets();
            requiredAcks = mode.requiredAcks(result.totalTargets());
            ackedTargets = result.ackedTargets();
            ackedNodeIds = result.ackedNodeIds();
            failedNodeIds = result.failedNodeIds();
            updatedAtEpochMillis = nowEpochMillis;
        }

        synchronized void markClosedFailure(String message, long nowEpochMillis) {
            if (finalized) {
                return;
            }
            SubmissionPhase previousPhase = phase;
            phase = SubmissionPhase.FAILED;
            updatedAtEpochMillis = nowEpochMillis;
            lastError = message;
            finalized = true;
            if (previousPhase != SubmissionPhase.FAILED && previousPhase != SubmissionPhase.COMMITTED) {
                owner.pendingCount.decrement();
            }
            owner.failedCount.increment();
            if (retryCount > 0L) {
                owner.retryingCount.decrement();
            }
            localOutcome.complete();
            committedOutcome.complete();
            owner.onFinalized(this);
        }

        boolean finalized() {
            return finalized;
        }

        SubmissionView snapshot(OrderStateView orderState) {
            return new SubmissionView(
                    submissionId,
                    idempotencyKey,
                    operationType,
                    userId,
                    orderId,
                    sequence,
                    phase,
                    localResult,
                    localDurable,
                    replicationRequired,
                    replicationCommitted,
                    totalTargets,
                    requiredAcks,
                    ackedTargets,
                    ackedNodeIds,
                    failedNodeIds,
                    retryCount,
                    lastError,
                    createdAtEpochMillis,
                    updatedAtEpochMillis,
                    orderState
            );
        }

        SubmissionReceipt receipt() {
            return new SubmissionReceipt(
                    submissionId,
                    idempotencyKey,
                    operationType,
                    userId,
                    orderId,
                    sequence,
                    phase,
                    localResult,
                    localDurable,
                    replicationRequired,
                    replicationCommitted,
                    totalTargets,
                    requiredAcks,
                    ackedTargets,
                    retryCount,
                    lastError,
                    createdAtEpochMillis,
                    updatedAtEpochMillis
            );
        }

        private SubmissionReceipt awaitLocalReceipt(long timeoutMillis) throws IOException {
            return awaitReceipt(localOutcome, timeoutMillis);
        }

        private SubmissionView awaitCommitted(long timeoutMillis, OrderStateTracker orderStateTracker) throws IOException {
            return awaitFuture(committedOutcome, timeoutMillis, orderStateTracker);
        }

        private SubmissionReceipt awaitReceipt(OutcomeSignal future, long timeoutMillis) throws IOException {
            try {
                if (!future.await(timeoutMillis)) {
                    return receipt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for submission state", e);
            } catch (RuntimeException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException io) {
                    throw io;
                }
                throw new IOException("submission state future failed", cause == null ? e : cause);
            }
            return receipt();
        }

        private SubmissionView awaitFuture(OutcomeSignal future,
                                           long timeoutMillis,
                                           OrderStateTracker orderStateTracker) throws IOException {
            try {
                if (!future.await(timeoutMillis)) {
                    return snapshot(orderStateTracker.find(orderId));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for submission state", e);
            } catch (RuntimeException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException io) {
                    throw io;
                }
                throw new IOException("submission state future failed", cause == null ? e : cause);
            }
            return snapshot(orderStateTracker.find(orderId));
        }
    }

    private static final class OutcomeSignal {
        private volatile boolean completed;
        private volatile Thread waiter;

        private boolean await(long timeoutMillis) throws InterruptedException {
            if (completed) {
                return true;
            }
            long remainingNanos = timeoutMillis <= 0L ? 0L : TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            long deadline = timeoutMillis <= 0L ? 0L : System.nanoTime() + remainingNanos;
            waiter = Thread.currentThread();
            while (!completed) {
                if (timeoutMillis <= 0L) {
                    waiter = null;
                    return false;
                }
                if (remainingNanos <= 0L) {
                    waiter = null;
                    return completed;
                }
                LockSupport.parkNanos(this, remainingNanos);
                if (Thread.interrupted()) {
                    waiter = null;
                    throw new InterruptedException();
                }
                remainingNanos = deadline - System.nanoTime();
            }
            waiter = null;
            return true;
        }

        private void complete() {
            completed = true;
            Thread parked = waiter;
            if (parked != null) {
                LockSupport.unpark(parked);
            }
        }
    }

    void onFinalized(TrackedSubmission tracked) {
        finalizedSubmissionIds.offer(tracked.submissionId());
        if (trackedCount.sum() <= maxTrackedSubmissions) {
            return;
        }
        synchronized (evictionLock) {
            evictFinalizedIfNeededLocked();
        }
    }

    private void evictFinalizedIfNeeded() {
        synchronized (evictionLock) {
            evictFinalizedIfNeededLocked();
        }
    }

    private void evictFinalizedIfNeededLocked() {
        while (trackedCount.sum() > maxTrackedSubmissions) {
            String oldest = finalizedSubmissionIds.poll();
            if (oldest == null) {
                return;
            }
            TrackedSubmission tracked = bySubmissionId.get(oldest);
            if (tracked == null || !tracked.finalized()) {
                continue;
            }
            bySubmissionId.remove(oldest, tracked);
            byIdempotencyKey.remove(tracked.idempotencyKey(), tracked);
            trackedCount.decrement();
            if (tracked.phase == SubmissionPhase.COMMITTED) {
                committedCount.decrement();
            } else if (tracked.phase == SubmissionPhase.FAILED) {
                failedCount.decrement();
            } else {
                pendingCount.decrement();
            }
        }
    }

    private static String normalizeIdempotencyKey(String idempotencyKey) {
        String normalized = Objects.requireNonNull(idempotencyKey, "idempotencyKey").trim();
        if (normalized.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        return normalized;
    }

    private static RequestFingerprint requestFingerprint(String operationType, Long userId, long orderId) {
        long hash = 0x9E3779B97F4A7C15L;
        hash = mix(hash, operationType.hashCode());
        hash = mix(hash, userId == null ? Long.MIN_VALUE : userId);
        return RequestFingerprint.generic(mix(hash, orderId));
    }

    static long mix(long hash, long value) {
        long x = value + 0x9E3779B97F4A7C15L + (hash << 6) + (hash >>> 2);
        return hash ^ x;
    }

    record RequestFingerprint(long hash,
                              byte side,
                              byte orderType,
                              byte timeInForce,
                              long price,
                              long quantity,
                              long ttlMillisOrMinValue) {
        static RequestFingerprint generic(long hash) {
            return new RequestFingerprint(hash, (byte) 0, (byte) 0, (byte) 0, 0L, 0L, Long.MIN_VALUE);
        }
    }
}
