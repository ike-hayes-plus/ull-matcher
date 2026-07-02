package io.github.ike.ullmatcher.ha.replication;

import java.io.IOException;

/**
 * 远端 standby 复制游标读取接口。
 * <p>
 * 用于 promotion readiness、复制 lag 观测和故障切换前的追平校验。
 */
public interface ReplicationCursorSource {
    /**
     * 目标节点标识。
     */
    String nodeId();

    /**
     * 读取目标 standby 当前复制游标。
     *
     * @param timeoutNanos 读取超时预算
     * @return 当前复制游标
     * @throws IOException 无法读取时抛出
     */
    ReplicationCursor fetchCursor(long timeoutNanos) throws IOException;
}
