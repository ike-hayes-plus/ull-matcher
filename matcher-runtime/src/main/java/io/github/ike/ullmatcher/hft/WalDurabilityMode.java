package io.github.ike.ullmatcher.hft;

/**
 * WAL 持久化策略。
 */
public enum WalDurabilityMode {
    /**
     * 每条命令追加后立即强制刷盘。
     */
    SYNC_PER_COMMAND,

    /**
     * 每累计一批命令后由提交线程执行一次强制刷盘。
     */
    SYNC_PER_BATCH,

    /**
     * 仅追加到内存映射页，依赖操作系统或显式 flush。
     */
    OS_BUFFERED
}
