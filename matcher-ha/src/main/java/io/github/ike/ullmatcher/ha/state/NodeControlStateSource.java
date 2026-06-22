package io.github.ike.ullmatcher.ha.state;

/**
 * 本地节点控制面状态提供者。
 */
public interface NodeControlStateSource {
    /**
     * 返回当前节点控制面状态。
     */
    NodeControlState currentState();
}
