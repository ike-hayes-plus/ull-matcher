package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.queue.MpscArrayQueue;
import io.github.ike.ullmatcher.server.telemetry.ReplicationCoordinatorMetricsSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 异步复制确认协调器。
 * <p>
 * 客户端提交在本地 WAL 落盘后立即返回受理凭证，复制确认由独立线程顺序推进，
 * 提交查询接口再暴露最终确认状态。
 */
final class ReplicationCoordinator implements Closeable {
    private static final long MIN_RETRY_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long MAX_RETRY_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
    private static final int MAX_BATCH_SIZE = 2_048;
    private static final int MAX_IN_FLIGHT_BATCHES = 16;
    private static final long BATCH_ACCUMULATION_NANOS = TimeUnit.MICROSECONDS.toNanos(200);

    private final MpscArrayQueue<ReplicationWork> queue;
    private final Thread worker;
    private final Clock clock;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong maxObservedQueueDepth = new AtomicLong();
    private final AtomicLong lastBatchSize = new AtomicLong();
    private final AtomicLong maxObservedBatchSize = new AtomicLong();
    private final AtomicLong batchesReplicatedTotal = new AtomicLong();
    private final AtomicLong commandsReplicatedTotal = new AtomicLong();
    private final AtomicLong lastCommittedSequence = new AtomicLong();
    private final AtomicLong retryCount = new AtomicLong();
    private final AtomicLong lastAccumulationMicros = new AtomicLong();
    private final AtomicLong lastCommitMicros = new AtomicLong();
    private final AtomicLong lastBackoffMicros = new AtomicLong();

    private volatile CommandReplicator replicator;
    private volatile ReplicationMode mode = ReplicationMode.LOCAL_ONLY;
    private volatile long timeoutNanos;
    private ReplicationWork deferredWork;

    ReplicationCoordinator(String nodeId) {
        this(nodeId, Clock.systemUTC());
    }

    ReplicationCoordinator(String nodeId, Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.queue = new MpscArrayQueue<>(1 << 16);
        this.worker = Thread.ofPlatform().name("matcher-replication-" + nodeId).start(this::runLoop);
    }

    void configure(CommandReplicator replicator, ReplicationMode mode, long timeoutNanos) {
        this.replicator = Objects.requireNonNull(replicator, "replicator");
        this.mode = Objects.requireNonNull(mode, "mode");
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        this.timeoutNanos = timeoutNanos;
    }

    void clear() {
        this.replicator = null;
        this.mode = ReplicationMode.LOCAL_ONLY;
        this.timeoutNanos = 0L;
    }

    void onLocalAccepted(Command command, SubmissionTracker.TrackedSubmission submission) {
        Objects.requireNonNull(command, "command");
        ReplicationMode requiredMode = mode;
        if (requiredMode == ReplicationMode.LOCAL_ONLY) {
            if (submission != null) {
                submission.markReplicationCommitted(new ReplicationResult(0, 0, java.util.List.of(), java.util.List.of()), requiredMode, clock.millis());
            }
            command.release();
            return;
        }
        enqueue(ReplicationWork.single(new ReplicationTask(command, submission, requiredMode, timeoutNanos)));
    }

    void onLocalAcceptedBatch(List<SubmissionRequest.PreparedSubmission> preparedSubmissions,
                              List<SubmissionTracker.TrackedSubmission> submissions) {
        Objects.requireNonNull(preparedSubmissions, "preparedSubmissions");
        Objects.requireNonNull(submissions, "submissions");
        if (preparedSubmissions.size() != submissions.size()) {
            throw new IllegalArgumentException("preparedSubmissions and submissions must have the same size");
        }
        if (preparedSubmissions.isEmpty()) {
            return;
        }
        ReplicationMode requiredMode = mode;
        if (requiredMode == ReplicationMode.LOCAL_ONLY) {
            long nowEpochMillis = clock.millis();
            ReplicationResult committed = new ReplicationResult(0, 0, java.util.List.of(), java.util.List.of());
            for (int i = 0; i < preparedSubmissions.size(); i++) {
                SubmissionTracker.TrackedSubmission submission = submissions.get(i);
                if (submission != null) {
                    submission.markReplicationCommitted(committed, requiredMode, nowEpochMillis);
                }
                preparedSubmissions.get(i).command().release();
            }
            return;
        }
        long timeoutNanos = this.timeoutNanos;
        ReplicationTask[] tasks = new ReplicationTask[preparedSubmissions.size()];
        for (int i = 0; i < preparedSubmissions.size(); i++) {
            tasks[i] = new ReplicationTask(
                    preparedSubmissions.get(i).command(),
                    submissions.get(i),
                    requiredMode,
                    timeoutNanos
            );
        }
        enqueue(ReplicationWork.batch(tasks));
    }

    void onLocalAcceptedBatch(SubmissionRequest.PreparedSubmission[] preparedSubmissions,
                              SubmissionTracker.TrackedSubmission[] submissions,
                              int size) {
        Objects.requireNonNull(preparedSubmissions, "preparedSubmissions");
        Objects.requireNonNull(submissions, "submissions");
        if (size < 0 || size > preparedSubmissions.length || size > submissions.length) {
            throw new IllegalArgumentException("size is out of bounds");
        }
        if (size == 0) {
            return;
        }
        ReplicationMode requiredMode = mode;
        if (requiredMode == ReplicationMode.LOCAL_ONLY) {
            long nowEpochMillis = clock.millis();
            ReplicationResult committed = new ReplicationResult(0, 0, java.util.List.of(), java.util.List.of());
            for (int i = 0; i < size; i++) {
                SubmissionTracker.TrackedSubmission submission = submissions[i];
                if (submission != null) {
                    submission.markReplicationCommitted(committed, requiredMode, nowEpochMillis);
                }
                preparedSubmissions[i].command().release();
            }
            return;
        }
        long timeoutNanos = this.timeoutNanos;
        ReplicationTask[] tasks = new ReplicationTask[size];
        for (int i = 0; i < size; i++) {
            tasks[i] = new ReplicationTask(
                    preparedSubmissions[i].command(),
                    submissions[i],
                    requiredMode,
                    timeoutNanos
            );
        }
        enqueue(ReplicationWork.batch(tasks));
    }

    /**
     * 提取 committed 主链的关键观测值。
     */
    ReplicationCoordinatorMetricsSnapshot metricsSnapshot() {
        return new ReplicationCoordinatorMetricsSnapshot(
                queue.size(),
                queue.capacity(),
                maxObservedQueueDepth.get(),
                lastBatchSize.get(),
                maxObservedBatchSize.get(),
                batchesReplicatedTotal.get(),
                commandsReplicatedTotal.get(),
                lastCommittedSequence.get(),
                retryCount.get(),
                lastAccumulationMicros.get(),
                lastCommitMicros.get(),
                lastBackoffMicros.get()
        );
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        worker.interrupt();
        try {
            worker.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while stopping replication coordinator", e);
        }
        ReplicationWork remaining;
        while ((remaining = queue.poll()) != null) {
            markWorkClosed(remaining);
        }
    }

    /**
     * committed watermark 推进主循环。
     * <p>
     * 这个循环的职责不是提高 accepted 吞吐，而是把 accepted 命令尽快收敛成 committed 结果，
     * 同时保证批次顺序和复制模式语义不被破坏。
     */
    private void runLoop() {
        ArrayDeque<InFlightBatch> inFlight = new ArrayDeque<>(MAX_IN_FLIGHT_BATCHES);
        BatchBuffer batchBuffer = new BatchBuffer(MAX_BATCH_SIZE);
        for (;;) {
            if (closed.get()) {
                drainQueuedWork();
                if (!inFlight.isEmpty()) {
                    abortInFlight(inFlight);
                }
                if (queue.size() == 0 && inFlight.isEmpty()) {
                    return;
                }
            }
            boolean progressed = false;
            progressed |= dispatchAvailableBatches(inFlight, batchBuffer);
            progressed |= progressCommittedWatermark(inFlight);
            if (!progressed) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
            }
        }
    }

    /**
     * 从待复制队列取出可发送批次，并填满当前 in-flight 窗口。
     */
    private boolean dispatchAvailableBatches(ArrayDeque<InFlightBatch> inFlight, BatchBuffer batchBuffer) {
        boolean dispatched = false;
        int maxInFlightBatches = preferredInFlightBatches();
        while (inFlight.size() < maxInFlightBatches) {
            BatchPollResult polled = pollBatch(batchBuffer);
            if (polled == null) {
                return dispatched;
            }
            InFlightBatch batch = new InFlightBatch(batchBuffer.copyTasks(), batchBuffer.size(), polled.requiredMode(), polled.timeoutNanos());
            submitAttempt(batch);
            inFlight.addLast(batch);
            dispatched = true;
        }
        return dispatched;
    }

    /**
     * 在限定窗口内聚合一批具有相同复制模式的工作项。
     */
    private BatchPollResult pollBatch(BatchBuffer batchBuffer) {
        ReplicationWork first = pollNextWork();
        if (first == null) {
            return null;
        }
        int maxBatchSize = preferredMaxBatchSize();
        long accumulationNanos = preferredAccumulationNanos();
        batchBuffer.clear();
        int addedFromFirst = addWorkToBatch(batchBuffer, first, maxBatchSize);
        if (addedFromFirst < first.size()) {
            deferredWork = first.sliceFrom(addedFromFirst);
        }
        long accumulationStarted = System.nanoTime();
        long accumulationDeadline = System.nanoTime() + accumulationNanos;
        while (batchBuffer.size() < maxBatchSize) {
            ReplicationWork next = pollNextWork();
            if (next == null) {
                if (System.nanoTime() >= accumulationDeadline) {
                    break;
                }
                Thread.onSpinWait();
                continue;
            }
            if (next.requiredMode() != first.requiredMode()) {
                deferredWork = next;
                break;
            }
            if (next.size() > maxBatchSize - batchBuffer.size()) {
                deferredWork = next;
                break;
            }
            addWorkToBatch(batchBuffer, next, maxBatchSize);
        }
        long batchSize = batchBuffer.size();
        lastBatchSize.set(batchSize);
        updateMax(maxObservedBatchSize, batchSize);
        lastAccumulationMicros.set(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - accumulationStarted));
        return new BatchPollResult(first.requiredMode(), first.taskAt(0).timeoutNanos());
    }

    private ReplicationWork pollNextWork() {
        ReplicationWork deferred = deferredWork;
        if (deferred != null) {
            deferredWork = null;
            return deferred;
        }
        return queue.poll();
    }

    private int preferredMaxBatchSize() {
        CommandReplicator currentReplicator = replicator;
        if (currentReplicator == null) {
            return MAX_BATCH_SIZE;
        }
        return Math.max(1, Math.min(MAX_BATCH_SIZE, currentReplicator.preferredMaxBatchSize()));
    }

    private int preferredInFlightBatches() {
        CommandReplicator currentReplicator = replicator;
        if (currentReplicator == null) {
            return MAX_IN_FLIGHT_BATCHES;
        }
        return Math.max(1, Math.min(MAX_IN_FLIGHT_BATCHES, currentReplicator.preferredInFlightBatches()));
    }

    private long preferredAccumulationNanos() {
        CommandReplicator currentReplicator = replicator;
        if (currentReplicator == null) {
            return BATCH_ACCUMULATION_NANOS;
        }
        return Math.max(0L, Math.min(BATCH_ACCUMULATION_NANOS, currentReplicator.preferredAccumulationNanos()));
    }

    private boolean progressCommittedWatermark(ArrayDeque<InFlightBatch> inFlight) {
        boolean progressed = false;
        long nowNanos = System.nanoTime();
        while (!inFlight.isEmpty()) {
            InFlightBatch head = inFlight.peekFirst();
            if (head.inFlightFuture == null) {
                if (nowNanos >= head.nextAttemptNanos) {
                    submitAttempt(head);
                    progressed = true;
                    continue;
                }
                break;
            }
            if (!head.inFlightFuture.isDone()) {
                break;
            }
            progressed = true;
            try {
                ReplicationResult result = head.inFlightFuture.join();
                lastCommitMicros.set(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - head.attemptStartedNanos));
                batchesReplicatedTotal.incrementAndGet();
                commandsReplicatedTotal.addAndGet(head.size);
                markBatchObservation(head.tasks, head.size, result, head.requiredMode);
                if (!result.satisfies(head.requiredMode)) {
                    scheduleRetry(head, new IOException("replication did not satisfy mode " + head.requiredMode));
                    break;
                }
                markBatchCommitted(head.tasks, head.size, result, head.requiredMode);
                lastCommittedSequence.set(head.tasks[head.size - 1].command().sequence);
                releaseBatch(head.tasks, head.size);
                inFlight.removeFirst();
            } catch (RuntimeException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (cause instanceof IOException io) {
                    scheduleRetry(head, io);
                } else {
                    scheduleRetry(head, new IOException("replication batch failed", cause));
                }
                break;
            }
        }
        return progressed;
    }

    private void submitAttempt(InFlightBatch batch) {
        CommandReplicator currentReplicator = replicator;
        if (currentReplicator == null) {
            scheduleRetry(batch, new IOException("replication pipeline is not configured"));
            return;
        }
        batch.attemptStartedNanos = System.nanoTime();
        try {
            batch.inFlightFuture = currentReplicator.replicateBatchAsync(batch.commandView, batch.requiredMode, batch.timeoutNanos);
        } catch (IOException e) {
            scheduleRetry(batch, e);
        }
    }

    private void scheduleRetry(InFlightBatch batch, IOException error) {
        markBatchRetry(batch.tasks, batch.size, error);
        lastBackoffMicros.set(TimeUnit.NANOSECONDS.toMicros(batch.backoffNanos));
        batch.inFlightFuture = null;
        batch.nextAttemptNanos = System.nanoTime() + batch.backoffNanos;
        batch.backoffNanos = Math.min(MAX_RETRY_BACKOFF_NANOS, batch.backoffNanos << 1);
    }

    private void enqueue(ReplicationWork work) {
        while (!closed.get()) {
            if (queue.offer(work)) {
                updateMax(maxObservedQueueDepth, queue.size());
                return;
            }
            Thread.onSpinWait();
        }
        markWorkClosed(work);
    }

    private void markBatchObservation(ReplicationTask[] tasks, int size, ReplicationResult result, ReplicationMode mode) {
        long nowEpochMillis = clock.millis();
        for (int i = 0; i < size; i++) {
            ReplicationTask task = tasks[i];
            if (task.submission() != null) {
                task.submission().markReplicationObservation(result, mode, nowEpochMillis);
            }
        }
    }

    private void markBatchCommitted(ReplicationTask[] tasks, int size, ReplicationResult result, ReplicationMode mode) {
        long nowEpochMillis = clock.millis();
        for (int i = 0; i < size; i++) {
            ReplicationTask task = tasks[i];
            if (task.submission() != null) {
                task.submission().markReplicationCommitted(result, mode, nowEpochMillis);
            }
        }
    }

    private void markBatchRetry(ReplicationTask[] tasks, int size, IOException error) {
        retryCount.incrementAndGet();
        long nowEpochMillis = clock.millis();
        for (int i = 0; i < size; i++) {
            ReplicationTask task = tasks[i];
            if (task.submission() != null) {
                task.submission().markReplicationRetry(error, nowEpochMillis);
            }
        }
    }

    private static void updateMax(AtomicLong currentMax, long candidate) {
        long observed = currentMax.get();
        while (candidate > observed && !currentMax.compareAndSet(observed, candidate)) {
            observed = currentMax.get();
        }
    }

    private static void releaseBatch(ReplicationTask[] tasks, int size) {
        for (int i = 0; i < size; i++) {
            tasks[i].command().release();
        }
    }

    private void abortInFlight(ArrayDeque<InFlightBatch> inFlight) {
        long nowEpochMillis = clock.millis();
        while (!inFlight.isEmpty()) {
            InFlightBatch batch = inFlight.removeFirst();
            if (batch.inFlightFuture != null) {
                batch.inFlightFuture.cancel(true);
            }
            for (int i = 0; i < batch.size; i++) {
                ReplicationTask task = batch.tasks[i];
                if (task.submission() != null) {
                    task.submission().markClosedFailure("replication coordinator is closed", nowEpochMillis);
                }
                task.command().release();
            }
        }
    }

    private void drainQueuedWork() {
        ReplicationWork deferred = deferredWork;
        deferredWork = null;
        if (deferred != null) {
            markWorkClosed(deferred);
        }
        ReplicationWork remaining;
        while ((remaining = queue.poll()) != null) {
            markWorkClosed(remaining);
        }
    }

    private void markWorkClosed(ReplicationWork work) {
        long nowEpochMillis = clock.millis();
        for (int i = 0; i < work.size(); i++) {
            ReplicationTask task = work.taskAt(i);
            if (task.submission() != null) {
                task.submission().markClosedFailure("replication coordinator is closed", nowEpochMillis);
            }
            task.command().release();
        }
    }

    private static int addWorkToBatch(BatchBuffer batch, ReplicationWork work, int maxBatchSize) {
        int remainingCapacity = maxBatchSize - batch.size();
        int taskCount = Math.min(remainingCapacity, work.size());
        for (int i = 0; i < taskCount; i++) {
            batch.add(work.taskAt(i));
        }
        return taskCount;
    }

    private static final class BatchBuffer {
        private final ReplicationTask[] tasks;
        private int size;

        private BatchBuffer(int capacity) {
            this.tasks = new ReplicationTask[capacity];
        }

        private void clear() {
            size = 0;
        }

        private int size() {
            return size;
        }

        private void add(ReplicationTask task) {
            tasks[size++] = task;
        }

        private ReplicationTask[] copyTasks() {
            ReplicationTask[] copy = new ReplicationTask[size];
            System.arraycopy(tasks, 0, copy, 0, size);
            return copy;
        }
    }

    private static final class InFlightBatch {
        private final ReplicationTask[] tasks;
        private final int size;
        private final List<Command> commandView;
        private final ReplicationMode requiredMode;
        private final long timeoutNanos;
        private long backoffNanos = MIN_RETRY_BACKOFF_NANOS;
        private long nextAttemptNanos;
        private long attemptStartedNanos;
        private CompletableFuture<ReplicationResult> inFlightFuture;

        private InFlightBatch(ReplicationTask[] tasks, int size, ReplicationMode requiredMode, long timeoutNanos) {
            this.tasks = tasks;
            this.size = size;
            this.commandView = new ReplicationCommandView(tasks, size);
            this.requiredMode = requiredMode;
            this.timeoutNanos = timeoutNanos;
        }
    }

    private record BatchPollResult(ReplicationMode requiredMode, long timeoutNanos) {
    }

    private record ReplicationTask(Command command,
                                   SubmissionTracker.TrackedSubmission submission,
                                   ReplicationMode requiredMode,
                                   long timeoutNanos) {
    }

    private interface ReplicationWork {
        int size();

        ReplicationMode requiredMode();

        ReplicationTask taskAt(int index);

        ReplicationWork sliceFrom(int index);

        static ReplicationWork single(ReplicationTask task) {
            return new SingleReplicationWork(task);
        }

        static ReplicationWork batch(ReplicationTask[] tasks) {
            return new BatchReplicationWork(tasks);
        }
    }

    private record SingleReplicationWork(ReplicationTask task) implements ReplicationWork {
        @Override
        public int size() {
            return 1;
        }

        @Override
        public ReplicationMode requiredMode() {
            return task.requiredMode();
        }

        @Override
        public ReplicationTask taskAt(int index) {
            if (index != 0) {
                throw new IndexOutOfBoundsException(index);
            }
            return task;
        }

        @Override
        public ReplicationWork sliceFrom(int index) {
            if (index == 0) {
                return this;
            }
            throw new IndexOutOfBoundsException(index);
        }
    }

    private record BatchReplicationWork(ReplicationTask[] tasks, int offset, int length) implements ReplicationWork {
        private BatchReplicationWork(ReplicationTask[] tasks) {
            this(tasks, 0, tasks.length);
        }

        private BatchReplicationWork {
            Objects.requireNonNull(tasks, "tasks");
            if (offset < 0 || length <= 0 || offset + length > tasks.length) {
                throw new IllegalArgumentException("invalid batch work slice");
            }
        }

        @Override
        public int size() {
            return length;
        }

        @Override
        public ReplicationMode requiredMode() {
            return tasks[offset].requiredMode();
        }

        @Override
        public ReplicationTask taskAt(int index) {
            if (index < 0 || index >= length) {
                throw new IndexOutOfBoundsException(index);
            }
            return tasks[offset + index];
        }

        @Override
        public ReplicationWork sliceFrom(int index) {
            if (index < 0 || index >= length) {
                throw new IndexOutOfBoundsException(index);
            }
            return new BatchReplicationWork(tasks, offset + index, length - index);
        }
    }

    private static final class ReplicationCommandView extends AbstractList<Command> {
        private final ReplicationTask[] tasks;
        private final int size;

        private ReplicationCommandView(ReplicationTask[] tasks, int size) {
            this.tasks = tasks;
            this.size = size;
        }

        @Override
        public Command get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(index);
            }
            return tasks[index].command();
        }

        @Override
        public int size() {
            return size;
        }
    }
}
