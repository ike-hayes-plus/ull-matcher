package io.github.ike.ullmatcher.ha.failover;

/**
 * 故障转移动作。
 */
public enum FailoverAction {
    KEEP_PRIMARY,
    HOLD,
    PROMOTE_STANDBY
}
