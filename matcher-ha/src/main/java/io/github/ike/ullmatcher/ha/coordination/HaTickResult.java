package io.github.ike.ullmatcher.ha.coordination;

import io.github.ike.ullmatcher.ha.failover.FailoverAction;

/**
 * 一次 HA 控制循环的结果。
 *
 * @param action 本次动作
 * @param roleBefore 变更前角色
 * @param roleAfter 变更后角色
 * @param leaseChanged 租约是否变更
 * @param reason 结果说明
 */
public record HaTickResult(
        FailoverAction action,
        HaRole roleBefore,
        HaRole roleAfter,
        boolean leaseChanged,
        String reason
) {}
