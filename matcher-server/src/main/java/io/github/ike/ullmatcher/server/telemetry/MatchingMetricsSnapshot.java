package io.github.ike.ullmatcher.server.telemetry;

/**
 * 撮合输出面的累计计数快照。
 */
public record MatchingMetricsSnapshot(
        long tradeCount,
        long orderEventCount,
        long rejectedCommandCount,
        long capacityRejectedCommandCount
) {
}
