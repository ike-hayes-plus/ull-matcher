package io.github.ike.ullmatcher.ha.readiness;

/**
 * standby 提升前检查结果。
 *
 * @param ready 是否可提升
 * @param receivedLag 接收滞后
 * @param durableLag 落盘滞后
 * @param appliedLag 重放滞后
 * @param snapshotLag 快照滞后
 * @param reason 结果原因
 */
public record PromotionReadinessReport(
        boolean ready,
        long receivedLag,
        long durableLag,
        long appliedLag,
        long snapshotLag,
        String reason
) {}
