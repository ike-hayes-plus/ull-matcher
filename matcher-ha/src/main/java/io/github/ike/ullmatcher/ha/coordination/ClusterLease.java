package io.github.ike.ullmatcher.ha.coordination;

import java.util.Objects;

/**
 * 分片主节点租约。
 *
 * @param ownerNodeId 当前租约持有者
 * @param fencingToken 当前纪元栅栏令牌
 * @param expiresAtNanos 租约到期时间
 */
public record ClusterLease(
        String ownerNodeId,
        FencingToken fencingToken,
        long expiresAtNanos
) {
    public ClusterLease {
        Objects.requireNonNull(ownerNodeId, "ownerNodeId");
        Objects.requireNonNull(fencingToken, "fencingToken");
        if (expiresAtNanos < 0L) {
            throw new IllegalArgumentException("expiresAtNanos must be non-negative");
        }
    }

    public boolean isExpired(long nowNanos) {
        if (nowNanos < 0L) {
            throw new IllegalArgumentException("nowNanos must be non-negative");
        }
        return nowNanos >= expiresAtNanos;
    }
}
