package io.github.ike.ullmatcher.ha.coordination;

/**
 * HA 节点角色。
 */
public enum HaRole {
    PRIMARY,
    STANDBY,
    CATCHING_UP,
    FENCED
}
