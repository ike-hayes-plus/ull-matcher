package io.github.ike.ullmatcher.ha.readiness;

/**
 * promotion readiness 阈值。
 *
 * @param maxReceivedLag 可接受的接收滞后
 * @param maxDurableLag 可接受的落盘滞后
 * @param maxAppliedLag 可接受的重放滞后
 * @param maxSnapshotLag 可接受的快照滞后
 */
public record PromotionReadinessPolicy(
        long maxReceivedLag,
        long maxDurableLag,
        long maxAppliedLag,
        long maxSnapshotLag
) {
    public PromotionReadinessPolicy {
        if (maxReceivedLag < 0L || maxDurableLag < 0L || maxAppliedLag < 0L || maxSnapshotLag < 0L) {
            throw new IllegalArgumentException("promotion readiness thresholds must be non-negative");
        }
    }

    public static PromotionReadinessPolicy strict() {
        return new PromotionReadinessPolicy(0L, 0L, 0L, 0L);
    }
}
