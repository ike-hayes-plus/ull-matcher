package io.github.ike.ullmatcher.ha.state;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.runtime.MatchLoopState;

import java.util.Objects;

/**
 * 节点内部控制面状态快照。
 *
 * @param nodeId 节点标识
 * @param role 当前 HA 角色
 * @param fencingToken 当前 fencing token
 * @param acceptingClientCommands 是否接受外部业务命令
 * @param loopState 撮合循环状态
 * @param processedCommandCount 已处理命令数
 * @param cursor 当前复制游标
 */
public record NodeControlState(
        String nodeId,
        HaRole role,
        FencingToken fencingToken,
        boolean acceptingClientCommands,
        MatchLoopState loopState,
        long processedCommandCount,
        ReplicationCursor cursor
) {
    public NodeControlState {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(fencingToken, "fencingToken");
        Objects.requireNonNull(loopState, "loopState");
        Objects.requireNonNull(cursor, "cursor");
        if (processedCommandCount < 0L) {
            throw new IllegalArgumentException("processedCommandCount must be non-negative");
        }
    }
}
