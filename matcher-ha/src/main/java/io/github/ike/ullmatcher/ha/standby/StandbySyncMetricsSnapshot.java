package io.github.ike.ullmatcher.ha.standby;

/**
 * standby 复制持久化与异步重放链路的运行指标快照。
 */
public record StandbySyncMetricsSnapshot(
        int applyQueueDepth,
        int applyQueueCapacity,
        long maxObservedApplyQueueDepth,
        long lastReplicatedBatchSize,
        long maxObservedReplicatedBatchSize,
        long replicatedBatchesTotal,
        long replicatedCommandsTotal,
        long ackFlushCount,
        long lastAckFlushCommands,
        long lastAckFlushMicros,
        long lastAckFlushIntervalMicros
) {
}
