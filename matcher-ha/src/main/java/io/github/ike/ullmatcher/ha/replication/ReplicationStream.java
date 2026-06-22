package io.github.ike.ullmatcher.ha.replication;

import io.github.ike.ullmatcher.api.Command;

import java.io.IOException;

/**
 * 持久连接上的复制流会话。
 */
public interface ReplicationStream extends AutoCloseable {
    /**
     * 发送一条命令到远端 standby。
     *
     * @param command 待复制命令
     * @throws IOException 发送或远端处理失败时抛出
     */
    void replicate(Command command) throws IOException;

    /**
     * 返回最近一次收到的确认游标。
     */
    ReplicationCursor lastAckedCursor();

    /**
     * 半关闭发送流，并等待远端返回最终游标。
     *
     * @return 最终确认游标
     * @throws IOException 关闭或等待确认失败时抛出
     */
    ReplicationCursor closeAndAwaitCursor() throws IOException;

    @Override
    void close() throws IOException;
}
