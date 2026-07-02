package io.github.ike.ullmatcher.ha.failover;

/**
 * 多备切换策略。
 *
 * @param primaryHeartbeatTimeoutNanos 主节点心跳超时时间
 * @param maxPromotionLag 最大允许切主滞后
 * @param minStandbyReplicas 最少待命副本数
 */
public record FailoverPolicy(
        long primaryHeartbeatTimeoutNanos,
        long maxPromotionLag,
        int minStandbyReplicas
) {
    public FailoverPolicy {
        if (primaryHeartbeatTimeoutNanos <= 0L) {
            throw new IllegalArgumentException("primaryHeartbeatTimeoutNanos must be positive");
        }
        if (maxPromotionLag < 0L) {
            throw new IllegalArgumentException("maxPromotionLag must be non-negative");
        }
        if (minStandbyReplicas <= 0) {
            throw new IllegalArgumentException("minStandbyReplicas must be positive");
        }
    }

    public static FailoverPolicy defaults() {
        return new FailoverPolicy(3_000_000_000L, 0L, 1);
    }
}
