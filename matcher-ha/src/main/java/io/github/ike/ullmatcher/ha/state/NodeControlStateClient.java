package io.github.ike.ullmatcher.ha.state;

import java.io.IOException;

/**
 * 远端节点控制面状态读取接口。
 */
public interface NodeControlStateClient {
    /**
     * 目标节点标识。
     */
    String nodeId();

    /**
     * 读取远端节点控制面状态。
     *
     * @param timeoutNanos 读取超时预算
     * @return 当前控制面状态
     * @throws IOException 读取失败时抛出
     */
    NodeControlState fetchNodeState(long timeoutNanos) throws IOException;
}
