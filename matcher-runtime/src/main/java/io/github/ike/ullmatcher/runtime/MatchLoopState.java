package io.github.ike.ullmatcher.runtime;

/**
 * 撮合循环状态。
 */
public enum MatchLoopState {
    STARTING,
    RUNNING,
    QUIESCING,
    DRAINING,
    STOPPED,
    FAILED
}
