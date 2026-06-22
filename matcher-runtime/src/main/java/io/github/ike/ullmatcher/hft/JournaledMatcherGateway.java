package io.github.ike.ullmatcher.hft;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.storage.wal.WalWriter;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * 写 WAL 并投递环形缓冲区的网关。
 * <p>
 * 典型生产链路：
 * <pre>{@code
 * API/网关线程 -> 风控/冻结 -> 定序器 -> 提交方法
 * }</pre>
 * <p>
 * 提交方法的顺序非常重要：
 * <ol>
 *     <li>先写 WAL 并强制刷盘，保证返回成功前具备本地可恢复性。</li>
 *     <li>再投递环形缓冲区，保证撮合线程只处理已经写入日志的命令。</li>
 * </ol>
 */
public final class JournaledMatcherGateway {
    /** 投递撮合环形缓冲区前使用的 WAL 写入器。 */
    private final WalWriter wal;

    /** 撮合器入站环形缓冲区。 */
    private final SpscRingBuffer<Command> ring;

    /** 让出线程前允许的发布失败次数。 */
    private final int spinLimit;

    /** 默认投递超时时间，单位为纳秒。 */
    private final long defaultOfferTimeoutNanos;

    /** 撮合循环是否仍可接收命令。 */
    private final BooleanSupplier acceptingSubmissions;

    /** WAL 持久化模式。 */
    private final WalDurabilityMode durabilityMode;

    /** 批量刷盘阈值。 */
    private final int forceBatchSize;

    /** 批量刷盘允许的最大延迟。 */
    private final long forceMaxDelayNanos;

    /** 最近一次提交结果。 */
    private volatile SubmitResult lastSubmitResult = SubmitResult.ACCEPTED;

    /** 成功投递数量。 */
    private long acceptedCount;

    /** WAL 成功追加数量。 */
    private long walAppendCount;

    /** WAL 强制刷盘数量。 */
    private long walForceCount;

    /** WAL 追加前失败数量。 */
    private long failedBeforeWalCount;

    /** WAL 追加后投递失败数量。 */
    private long failedAfterWalCount;

    /** 自上次刷盘后累计的追加数。 */
    private long appendedSinceForce;

    /** 最近一次强制刷盘时间。 */
    private long lastForceAtNanos;

    /**
     * 创建带日志的网关。
     *
     * @param wal WAL 写入器
     * @param ring 撮合器入站环形缓冲区
     * @param spinLimit 让出线程前允许的发布失败次数
     */
    public JournaledMatcherGateway(WalWriter wal, SpscRingBuffer<Command> ring, int spinLimit) {
        this(wal, ring, spinLimit, TimeUnit.SECONDS.toNanos(1), () -> true, WalDurabilityMode.SYNC_PER_COMMAND, 1, 0L);
    }

    /**
     * 创建带日志、超时和健康检查的网关。
     *
     * @param wal WAL 写入器
     * @param ring 撮合器入站环形缓冲区
     * @param spinLimit 让出线程前允许的发布失败次数
     * @param defaultOfferTimeoutNanos 默认投递超时时间，负数表示一直等待
     * @param acceptingSubmissions 撮合循环健康检查
     */
    public JournaledMatcherGateway(WalWriter wal, SpscRingBuffer<Command> ring, int spinLimit,
                                   long defaultOfferTimeoutNanos, BooleanSupplier acceptingSubmissions) {
        this(wal, ring, spinLimit, defaultOfferTimeoutNanos, acceptingSubmissions, WalDurabilityMode.SYNC_PER_COMMAND, 1, 0L);
    }

    /**
     * 创建带日志、超时、健康检查和 WAL 持久化策略的网关。
     *
     * @param wal WAL 写入器
     * @param ring 撮合器入站环形缓冲区
     * @param spinLimit 让出线程前允许的发布失败次数
     * @param defaultOfferTimeoutNanos 默认投递超时时间，负数表示一直等待
     * @param acceptingSubmissions 撮合循环健康检查
     * @param durabilityMode WAL 持久化模式
     * @param forceBatchSize 批量刷盘阈值，仅在 {@link WalDurabilityMode#SYNC_PER_BATCH} 生效
     * @param forceMaxDelayMicros 批量刷盘允许的最大延迟，0 表示仅按批量阈值触发
     */
    public JournaledMatcherGateway(WalWriter wal, SpscRingBuffer<Command> ring, int spinLimit,
                                   long defaultOfferTimeoutNanos, BooleanSupplier acceptingSubmissions,
                                   WalDurabilityMode durabilityMode, int forceBatchSize, long forceMaxDelayMicros) {
        if (spinLimit <= 0) {
            throw new IllegalArgumentException("spinLimit must be positive");
        }
        if (forceBatchSize <= 0) {
            throw new IllegalArgumentException("forceBatchSize must be positive");
        }
        if (forceMaxDelayMicros < 0L) {
            throw new IllegalArgumentException("forceMaxDelayMicros must not be negative");
        }
        this.wal = Objects.requireNonNull(wal, "wal");
        this.ring = Objects.requireNonNull(ring, "ring");
        this.spinLimit = spinLimit;
        this.defaultOfferTimeoutNanos = defaultOfferTimeoutNanos;
        this.acceptingSubmissions = Objects.requireNonNull(acceptingSubmissions, "acceptingSubmissions");
        this.durabilityMode = Objects.requireNonNull(durabilityMode, "durabilityMode");
        this.forceBatchSize = forceBatchSize;
        this.forceMaxDelayNanos = TimeUnit.MICROSECONDS.toNanos(forceMaxDelayMicros);
        this.lastForceAtNanos = System.nanoTime();
    }

    /**
     * 先强制持久化命令，再发布到撮合环形缓冲区。
     *
     * @param command 待提交命令
     * @throws IOException WAL 追加失败时抛出
     * @throws IllegalStateException 命令无法投递撮合线程时抛出
     */
    public void submit(Command command) throws IOException {
        SubmitResult result = trySubmit(command, defaultOfferTimeoutNanos);
        if (result != SubmitResult.ACCEPTED) {
            throw new IllegalStateException("gateway submit failed: " + result + ", walAppended=" + result.walAppended());
        }
    }

    /**
     * 先强制持久化命令，再在指定时间内尝试发布到撮合环形缓冲区。
     * <p>
     * 如果返回值的 {@link SubmitResult#walAppended()} 为 {@code true}，说明命令已经进入 WAL；
     * 调用方不应直接生成新序列号重试，而应按恢复或补偿流程处理。
     *
     * @param command 待提交命令
     * @param offerTimeoutNanos 投递超时时间；负数表示一直等待，0 表示只尝试一次
     * @return 提交结果
     * @throws IOException WAL 追加失败时抛出
     */
    public SubmitResult trySubmit(Command command, long offerTimeoutNanos) throws IOException {
        return trySubmitBatch(List.of(command), offerTimeoutNanos);
    }

    /**
     * 先强制持久化一批命令，再在指定时间内尝试把整批命令发布到撮合环形缓冲区。
     * <p>
     * 该方法会先等待足够的环形缓冲区容量，再写 WAL。这样批量提交返回成功时，
     * 整批命令都已经满足“已落盘且已进入撮合线程”的一致语义。
     *
     * @param commands 待提交命令
     * @param offerTimeoutNanos 投递超时时间；负数表示一直等待，0 表示只尝试一次
     * @return 提交结果
     * @throws IOException WAL 追加失败时抛出
     */
    public SubmitResult trySubmitBatch(List<Command> commands, long offerTimeoutNanos) throws IOException {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return record(SubmitResult.ACCEPTED);
        }
        if (!acceptingSubmissions.getAsBoolean()) {
            failedBeforeWalCount += commands.size();
            return record(SubmitResult.MATCHER_NOT_RUNNING);
        }
        long start = offerTimeoutNanos >= 0 ? System.nanoTime() : 0L;
        int offset = 0;
        while (offset < commands.size()) {
            int remaining = commands.size() - offset;
            int chunkSize;
            try {
                chunkSize = awaitRingChunkCapacity(remaining, offerTimeoutNanos, start);
            } catch (GatewayCapacityException e) {
                if (e.result == SubmitResult.MATCHER_NOT_RUNNING || e.result == SubmitResult.RING_FULL_BEFORE_WAL_APPEND) {
                    failedBeforeWalCount += remaining;
                }
                return record(e.result);
            }
            if (!acceptingSubmissions.getAsBoolean()) {
                failedBeforeWalCount += remaining;
                return record(SubmitResult.MATCHER_NOT_RUNNING);
            }
            List<Command> chunk = commands.subList(offset, offset + chunkSize);
            for (Command command : chunk) {
                Objects.requireNonNull(command, "command");
            }
            wal.appendAll(chunk);
            walAppendCount += chunkSize;
            appendedSinceForce += chunkSize;
            forceIfRequired(chunkSize);
            if (!acceptingSubmissions.getAsBoolean()) {
                failedAfterWalCount += remaining;
                return record(SubmitResult.MATCHER_STOPPED_AFTER_WAL_APPEND);
            }
            publishBatch(chunk);
            acceptedCount += chunkSize;
            offset += chunkSize;
        }
        return record(SubmitResult.ACCEPTED);
    }

    private int awaitRingChunkCapacity(int remainingCommands, long offerTimeoutNanos, long startNanos) {
        int spins = 0;
        while (true) {
            int available = ring.remainingCapacity();
            if (available > 0) {
                return Math.min(remainingCommands, available);
            }
            if (!acceptingSubmissions.getAsBoolean()) {
                throw new GatewayCapacityException(SubmitResult.MATCHER_NOT_RUNNING);
            }
            if (offerTimeoutNanos == 0 || (offerTimeoutNanos > 0 && System.nanoTime() - startNanos >= offerTimeoutNanos)) {
                throw new GatewayCapacityException(SubmitResult.RING_FULL_BEFORE_WAL_APPEND);
            }
            if (++spins >= spinLimit) {
                Thread.yield();
                spins = 0;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private void publishBatch(List<Command> batch) {
        for (Command command : batch) {
            command.retain();
            if (!ring.offer(command)) {
                command.release();
                failedAfterWalCount += batch.size();
                throw new IllegalStateException("ring capacity changed after batch reservation");
            }
        }
    }

    /**
     * 将 WAL 数据强制刷到持久化存储。
     *
     * @throws IOException 强制刷盘失败时抛出
     */
    public void flushWal() throws IOException {
        doForce();
    }

    /**
     * 返回最近一次提交结果。
     *
     * @return 最近提交结果
     */
    public SubmitResult lastSubmitResult() {
        return lastSubmitResult;
    }

    /**
     * 返回成功投递数量。
     *
     * @return 成功投递数量
     */
    public long acceptedCount() {
        return acceptedCount;
    }

    /**
     * 返回 WAL 成功追加数量。
     *
     * @return WAL 追加数量
     */
    public long walAppendCount() {
        return walAppendCount;
    }

    /**
     * 返回 WAL 强制刷盘数量。
     *
     * @return WAL 强制刷盘数量
     */
    public long walForceCount() {
        return walForceCount;
    }

    /**
     * 返回 WAL 追加前失败数量。
     *
     * @return 追加前失败数量
     */
    public long failedBeforeWalCount() {
        return failedBeforeWalCount;
    }

    /**
     * 返回 WAL 追加后投递失败数量。
     *
     * @return 追加后失败数量
     */
    public long failedAfterWalCount() {
        return failedAfterWalCount;
    }

    /**
     * 记录最近提交结果。
     *
     * @param result 提交结果
     * @return 原提交结果
     */
    private SubmitResult record(SubmitResult result) {
        lastSubmitResult = result;
        return result;
    }

    private void forceIfRequired(int appendedCommands) throws IOException {
        switch (durabilityMode) {
            case SYNC_PER_COMMAND -> {
                if (appendedCommands > 0) {
                    doForce();
                }
            }
            case SYNC_PER_BATCH -> {
                long now = System.nanoTime();
                if (appendedSinceForce >= forceBatchSize
                        || (forceMaxDelayNanos > 0L && appendedSinceForce > 0L && now - lastForceAtNanos >= forceMaxDelayNanos)) {
                    doForce();
                }
            }
            case OS_BUFFERED -> {
                // 明确不在提交路径中强制刷盘。
            }
        }
    }

    private static final class GatewayCapacityException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private final SubmitResult result;

        private GatewayCapacityException(SubmitResult result) {
            super(result.name());
            this.result = result;
        }
    }

    private void doForce() throws IOException {
        wal.force();
        walForceCount++;
        appendedSinceForce = 0L;
        lastForceAtNanos = System.nanoTime();
    }
}
