package io.github.ike.ullmatcher.ha.state;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;

import java.util.Objects;

/**
 * 单个副本的控制面状态。
 *
 * @param nodeId 节点标识
 * @param role 当前角色
 * @param reachable 控制面是否可达
 * @param healthy 节点自检是否健康
 * @param lastHeartbeatNanos 最近心跳时间
 * @param fencingToken 栅栏令牌
 * @param cursor 复制进度
 */
public record ReplicaState(
        String nodeId,
        HaRole role,
        boolean reachable,
        boolean healthy,
        long lastHeartbeatNanos,
        FencingToken fencingToken,
        ReplicationCursor cursor
) {
    public ReplicaState {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(fencingToken, "fencingToken");
        Objects.requireNonNull(cursor, "cursor");
        if (lastHeartbeatNanos < 0L) {
            throw new IllegalArgumentException("lastHeartbeatNanos must be non-negative");
        }
    }
}
