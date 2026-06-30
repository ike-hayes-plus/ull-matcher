package io.github.ike.ullmatcher.ha.standby;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.replication.ReplicationTarget;
import io.github.ike.ullmatcher.queue.MpscArrayQueue;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.storage.wal.WalWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * standby 节点的 WAL 同步与重放服务。
 * <p>
 * 复制确认以 WAL 已持久化为准，应用进度通过异步重放线程推进。
 * 这样主节点可以以持久化水位完成复制确认，而晋升与健康判定继续使用应用水位。
 */
public final class StandbySyncService implements ReplicationTarget, Closeable {
    private static final int OFFER_SPIN_LIMIT = 1_024;
    private static final int MIN_APPLY_QUEUE_CAPACITY = 1 << 10;
    private static final int DEFAULT_APPLY_BATCH_SIZE = 256;

    private final String nodeId;
    private final WalWriter wal;
    private final SpscRingBuffer<Command> ring;
    private final UltraLowLatencyMatcher matcher;
    private final StandbySyncConfig config;
    private final MpscArrayQueue<Command> applyQueue;
    private final Thread applyThread;
    private final AtomicLong maxObservedApplyQueueDepth = new AtomicLong();
    private final AtomicLong lastReplicatedBatchSize = new AtomicLong();
    private final AtomicLong maxObservedReplicatedBatchSize = new AtomicLong();
    private final AtomicLong replicatedBatchesTotal = new AtomicLong();
    private final AtomicLong replicatedCommandsTotal = new AtomicLong();
    private final AtomicLong ackFlushCount = new AtomicLong();
    private final AtomicLong lastAckFlushCommands = new AtomicLong();
    private final AtomicLong lastAckFlushMicros = new AtomicLong();
    private final AtomicLong lastAckFlushIntervalMicros = new AtomicLong();
    private final AtomicLong lastAckFlushNanos = new AtomicLong();

    private volatile ReplicationCursor cursor = new ReplicationCursor(0L, 0L, 0L, 0L);
    private volatile boolean running = true;
    private volatile IOException applyFailure;

    public StandbySyncService(String nodeId,
                              WalWriter wal,
                              SpscRingBuffer<Command> ring,
                              UltraLowLatencyMatcher matcher,
                              StandbySyncConfig config) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.wal = Objects.requireNonNull(wal, "wal");
        this.ring = Objects.requireNonNull(ring, "ring");
        this.matcher = Objects.requireNonNull(matcher, "matcher");
        this.config = Objects.requireNonNull(config, "config");
        if (config.applyToMatcher()) {
            this.applyQueue = new MpscArrayQueue<>(applyQueueCapacity(ring.capacity()));
            this.applyThread = Thread.ofPlatform().name("standby-apply-" + nodeId).start(this::applyLoop);
        } else {
            this.applyQueue = null;
            this.applyThread = null;
        }
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public synchronized void replicate(Command command, long timeoutNanos) throws IOException {
        Objects.requireNonNull(command, "command");
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        long beforeReceived = cursor.lastReceivedSequence();
        appendReplicated(command, timeoutNanos);
        if (config.forceOnEveryCommand() && cursor.lastReceivedSequence() > beforeReceived) {
            force(1);
        }
    }

    @Override
    public synchronized void replicateBatch(List<Command> commands, long timeoutNanos) throws IOException {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return;
        }
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        long beforeReceived = cursor.lastReceivedSequence();
        appendReplicatedBatch(commands, timeoutNanos);
        if (config.forceOnEveryCommand() && cursor.lastReceivedSequence() > beforeReceived) {
            force(commands.size());
        }
    }

    /**
     * 只推进 standby 的已接收水位，不在这里强制 durable flush。
     * <p>
     * 上层可以先攒批，再统一决定 force/ack 节奏。
     */
    public synchronized void appendReplicated(Command command, long timeoutNanos) throws IOException {
        Objects.requireNonNull(command, "command");
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        ensureApplyHealthy();
        if (command.sequence <= cursor.lastReceivedSequence()) {
            return;
        }
        validateNextSequence(command.sequence);
        wal.append(command);
        recordReplicatedBatch(1);
        long received = command.sequence;
        long applied = cursor.lastAppliedSequence();
        long durable = cursor.lastDurableSequence();
        cursor = new ReplicationCursor(received, durable, applied, cursor.snapshotSequence());
        if (config.applyToMatcher()) {
            enqueueForApply(command, timeoutNanos);
        }
    }

    /**
     * 批量追加复制命令。
     * <p>
     * 这是 standby 侧降低 committed tail 的关键路径：先批量写 WAL，再统一 durable/ack。
     */
    public synchronized void appendReplicatedBatch(List<Command> commands, long timeoutNanos) throws IOException {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return;
        }
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        ensureApplyHealthy();
        long alreadyReceived = cursor.lastReceivedSequence();
        int firstNewIndex = 0;
        while (firstNewIndex < commands.size() && commands.get(firstNewIndex).sequence <= alreadyReceived) {
            firstNewIndex++;
        }
        if (firstNewIndex == commands.size()) {
            return;
        }
        long received = expectedNextSequence() - 1L;
        for (int i = firstNewIndex; i < commands.size(); i++) {
            Command command = commands.get(i);
            Objects.requireNonNull(command, "command");
            validateNextSequence(command.sequence, received + 1L);
            received = command.sequence;
        }
        List<Command> newCommands = commands.subList(firstNewIndex, commands.size());
        wal.appendAll(newCommands);
        recordReplicatedBatch(newCommands.size());
        long applied = cursor.lastAppliedSequence();
        long durable = cursor.lastDurableSequence();
        cursor = new ReplicationCursor(received, durable, applied, cursor.snapshotSequence());
        if (config.applyToMatcher()) {
            for (Command command : newCommands) {
                enqueueForApply(command, timeoutNanos);
            }
        }
    }

    public synchronized void force() throws IOException {
        force(0);
    }

    /**
     * 把当前已接收命令统一刷成 durable，并推进 durable 水位。
     */
    public synchronized void force(int flushedCommands) throws IOException {
        forceAndRecordAckFlush(flushedCommands);
        ReplicationCursor current = cursor;
        cursor = new ReplicationCursor(
                current.lastReceivedSequence(),
                current.lastReceivedSequence(),
                current.lastAppliedSequence(),
                current.snapshotSequence()
        );
    }

    public synchronized void markSnapshot(long snapshotSequence) {
        if (snapshotSequence < 0L) {
            throw new IllegalArgumentException("snapshotSequence must be non-negative");
        }
        ReplicationCursor current = cursor;
        long appliedWatermark = matcher.lastSequence();
        cursor = new ReplicationCursor(
                Math.max(current.lastReceivedSequence(), appliedWatermark),
                Math.max(current.lastDurableSequence(), appliedWatermark),
                Math.max(current.lastAppliedSequence(), appliedWatermark),
                snapshotSequence
        );
    }

    public ReplicationCursor cursor() {
        return cursor;
    }

    public StandbySyncMetricsSnapshot metricsSnapshot() {
        return new StandbySyncMetricsSnapshot(
                applyQueue == null ? 0 : applyQueue.size(),
                applyQueue == null ? 0 : applyQueue.capacity(),
                maxObservedApplyQueueDepth.get(),
                lastReplicatedBatchSize.get(),
                maxObservedReplicatedBatchSize.get(),
                replicatedBatchesTotal.get(),
                replicatedCommandsTotal.get(),
                ackFlushCount.get(),
                lastAckFlushCommands.get(),
                lastAckFlushMicros.get(),
                lastAckFlushIntervalMicros.get()
        );
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (applyThread == null) {
            return;
        }
        try {
            applyThread.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while stopping standby apply thread", e);
        }
        ensureApplyHealthy();
    }

    private void publishToMatchLoop(Command command, long timeoutNanos) throws IOException {
        long deadline = timeoutNanos > 0L ? System.nanoTime() + timeoutNanos : Long.MAX_VALUE;
        int spins = 0;
        while (!ring.offer(command)) {
            if (System.nanoTime() >= deadline) {
                throw new IOException("standby replication ring is full");
            }
            if (++spins >= OFFER_SPIN_LIMIT) {
                Thread.yield();
                spins = 0;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private void enqueueForApply(Command command, long timeoutNanos) throws IOException {
        long deadline = timeoutNanos > 0L ? System.nanoTime() + timeoutNanos : Long.MAX_VALUE;
        int spins = 0;
        while (!applyQueue.offer(command)) {
            ensureApplyHealthy();
            if (System.nanoTime() >= deadline) {
                throw new IOException("standby apply backlog is full");
            }
            if (++spins >= OFFER_SPIN_LIMIT) {
                Thread.yield();
                spins = 0;
            } else {
                Thread.onSpinWait();
            }
        }
        updateMax(maxObservedApplyQueueDepth, applyQueue.size());
    }

    private void awaitAppliedSequence(long sequence, long timeoutNanos) throws IOException {
        long deadline = timeoutNanos > 0L ? System.nanoTime() + timeoutNanos : Long.MAX_VALUE;
        int spins = 0;
        while (matcher.lastSequence() < sequence) {
            if (System.nanoTime() >= deadline) {
                throw new IOException("timed out waiting for standby apply sequence " + sequence);
            }
            if (++spins >= OFFER_SPIN_LIMIT) {
                Thread.yield();
                spins = 0;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    /**
     * 异步重放线程。
     * <p>
     * durable 确认和应用推进分离后，主节点可以按 durable 水位完成复制确认，
     * 而晋升、安全检查继续看 applied 水位。
     */
    private void applyLoop() {
        Command[] batch = new Command[Math.min(DEFAULT_APPLY_BATCH_SIZE, ring.capacity())];
        long consecutiveIdleCount = 0L;
        while (running || applyQueue.size() > 0) {
            Command command = applyQueue.poll();
            if (command == null) {
                consecutiveIdleCount++;
                idle(consecutiveIdleCount);
                continue;
            }
            consecutiveIdleCount = 0L;
            try {
                int batchSize = drainApplyBatch(batch, command);
                long highestSequence = publishApplyBatch(batch, batchSize);
                awaitAppliedSequence(highestSequence, 0L);
                markApplied(highestSequence);
                clearBatch(batch, batchSize);
            } catch (IOException e) {
                applyFailure = e;
                running = false;
                return;
            } catch (RuntimeException e) {
                applyFailure = new IOException("standby apply loop failed", e);
                running = false;
                return;
            }
        }
    }

    private int drainApplyBatch(Command[] batch, Command first) {
        batch[0] = first;
        int batchSize = 1;
        while (batchSize < batch.length) {
            Command next = applyQueue.poll();
            if (next == null) {
                break;
            }
            batch[batchSize++] = next;
        }
        return batchSize;
    }

    private long publishApplyBatch(Command[] batch, int batchSize) throws IOException {
        long highestSequence = 0L;
        for (int i = 0; i < batchSize; i++) {
            Command command = batch[i];
            publishToMatchLoop(command, 0L);
            highestSequence = command.sequence;
        }
        return highestSequence;
    }

    private static void clearBatch(Command[] batch, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            batch[i] = null;
        }
    }

    private synchronized void markApplied(long sequence) {
        ReplicationCursor current = cursor;
        if (sequence <= current.lastAppliedSequence()) {
            return;
        }
        cursor = new ReplicationCursor(
                current.lastReceivedSequence(),
                current.lastDurableSequence(),
                sequence,
                current.snapshotSequence()
        );
    }

    private void ensureApplyHealthy() throws IOException {
        IOException failure = applyFailure;
        if (failure != null) {
            throw new IOException("standby apply loop failed", failure);
        }
    }

    private long expectedNextSequence() {
        long current = Math.max(cursor.lastReceivedSequence(), cursor.snapshotSequence());
        return current + 1L;
    }

    private void validateNextSequence(long sequence) throws IOException {
        validateNextSequence(sequence, expectedNextSequence());
    }

    private static void validateNextSequence(long sequence, long expected) throws IOException {
        if (sequence != expected) {
            throw new IOException("replicated command sequence gap: expected " + expected + " but received " + sequence);
        }
    }

    private static int applyQueueCapacity(int ringCapacity) {
        return nextPowerOfTwo(Math.max(MIN_APPLY_QUEUE_CAPACITY, ringCapacity << 3));
    }

    private void recordReplicatedBatch(int batchSize) {
        lastReplicatedBatchSize.set(batchSize);
        updateMax(maxObservedReplicatedBatchSize, batchSize);
        replicatedBatchesTotal.incrementAndGet();
        replicatedCommandsTotal.addAndGet(batchSize);
    }

    private void forceAndRecordAckFlush(int flushedCommands) throws IOException {
        long started = System.nanoTime();
        wal.force();
        long completed = System.nanoTime();
        lastAckFlushMicros.set(TimeUnit.NANOSECONDS.toMicros(completed - started));
        lastAckFlushCommands.set(flushedCommands);
        ackFlushCount.incrementAndGet();
        long previous = lastAckFlushNanos.getAndSet(completed);
        if (previous != 0L) {
            lastAckFlushIntervalMicros.set(TimeUnit.NANOSECONDS.toMicros(completed - previous));
        }
    }

    private static void updateMax(AtomicLong currentMax, long candidate) {
        long observed = currentMax.get();
        while (candidate > observed && !currentMax.compareAndSet(observed, candidate)) {
            observed = currentMax.get();
        }
    }

    private static int nextPowerOfTwo(int value) {
        int normalized = Math.max(2, value);
        return 1 << (32 - Integer.numberOfLeadingZeros(normalized - 1));
    }

    private static void idle(long consecutiveIdleCount) {
        if (consecutiveIdleCount <= 64L) {
            Thread.onSpinWait();
            return;
        }
        if (consecutiveIdleCount <= 128L) {
            Thread.yield();
            return;
        }
        java.util.concurrent.locks.LockSupport.parkNanos(1_000L);
    }
}
