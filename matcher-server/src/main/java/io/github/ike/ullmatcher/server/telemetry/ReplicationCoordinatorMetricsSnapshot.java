package io.github.ike.ullmatcher.server.telemetry;

/**
 * 主节点复制确认协调器的队列、批量与提交耗时快照。
 */
public record ReplicationCoordinatorMetricsSnapshot(
        int queueDepth,
        int queueCapacity,
        long maxObservedQueueDepth,
        long lastBatchSize,
        long maxObservedBatchSize,
        long batchesReplicatedTotal,
        long commandsReplicatedTotal,
        long lastCommittedSequence,
        long retryCount,
        long lastAccumulationMicros,
        long lastCommitMicros,
        long lastBackoffMicros
) {
}
