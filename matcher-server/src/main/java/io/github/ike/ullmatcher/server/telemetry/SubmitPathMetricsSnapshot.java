package io.github.ike.ullmatcher.server.telemetry;

/**
 * 提交流水线的瞬时状态与累计计数。
 */
public record SubmitPathMetricsSnapshot(
        int submitQueueDepth,
        int submitQueueCapacity,
        int ringDepth,
        int ringRemainingCapacity,
        long walAcceptedTotal,
        long walAppendedTotal,
        long walForcedTotal,
        long failedBeforeWalTotal,
        long failedAfterWalTotal,
        String lastSubmitResult
) {
}
