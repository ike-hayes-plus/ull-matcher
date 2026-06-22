package io.github.ike.ullmatcher.hft;

/**
 * 带 WAL 网关提交结果。
 */
public enum SubmitResult {
    /** 命令已强制写入本地 WAL 并成功投递撮合环形缓冲区。 */
    ACCEPTED(true),

    /** 撮合循环已不可用，命令未写入 WAL。 */
    MATCHER_NOT_RUNNING(false),

    /** 环形缓冲区在超时时间内没有足够容量，命令未写入 WAL。 */
    RING_FULL_BEFORE_WAL_APPEND(false),

    /** 预分配命令槽位暂时耗尽，命令未写入 WAL。 */
    COMMAND_POOL_EXHAUSTED(false),

    /** 命令已写入 WAL，但投递前撮合循环变为不可用。 */
    MATCHER_STOPPED_AFTER_WAL_APPEND(true),

    /** 命令已写入 WAL，但环形缓冲区在超时时间内仍然满。 */
    RING_FULL_AFTER_WAL_APPEND(true);

    /** 返回时命令是否已经进入本地 WAL。 */
    private final boolean walAppended;

    /**
     * 创建提交结果。
     *
     * @param walAppended 返回该结果时命令是否已经写入 WAL
     */
    SubmitResult(boolean walAppended) {
        this.walAppended = walAppended;
    }

    /**
     * 判断该结果是否表示命令已经进入本地 WAL。
     *
     * @return 已进入本地 WAL 时返回 {@code true}
     */
    public boolean walAppended() {
        return walAppended;
    }
}
