package io.github.ike.ullmatcher.server.telemetry;

/**
 * 提交查询面的当前状态分布。
 */
public record SubmissionMetricsSnapshot(
        long trackedCount,
        long pendingCount,
        long committedCount,
        long failedCount,
        long retryingCount
) {
}
