package io.github.ike.ullmatcher.ha.replication;

import io.github.ike.ullmatcher.api.Command;

import java.io.IOException;
import java.util.List;

/**
 * 单个 standby 复制目标。
 */
public interface ReplicationTarget {
    /**
     * 目标节点标识。
     */
    String nodeId();

    /**
     * 复制一条命令到目标节点。
     *
     * @param command 待复制命令
     * @param timeoutNanos 复制超时预算
     * @throws IOException 复制失败时抛出
     */
    void replicate(Command command, long timeoutNanos) throws IOException;

    /**
     * 批量复制多条命令到目标节点。
     *
     * @param commands 待复制命令，调用方保证顺序已经定好
     * @param timeoutNanos 复制超时预算
     * @throws IOException 复制失败时抛出
     */
    default void replicateBatch(List<Command> commands, long timeoutNanos) throws IOException {
        if (commands == null || commands.isEmpty()) {
            return;
        }
        for (Command command : commands) {
            replicate(command, timeoutNanos);
        }
    }
}
