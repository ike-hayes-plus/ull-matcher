package io.github.ike.ullmatcher.ha.replication;

import java.io.IOException;

/**
 * 支持长连接流复制的 standby 目标。
 */
public interface StreamingReplicationTarget {
    /**
     * 目标节点标识。
     */
    String nodeId();

    /**
     * 打开新的复制流会话。
     *
     * @param timeoutNanos 连接和流确认超时预算
     * @return 复制流会话
     * @throws IOException 建立流失败时抛出
     */
    ReplicationStream openReplicationStream(long timeoutNanos) throws IOException;
}
