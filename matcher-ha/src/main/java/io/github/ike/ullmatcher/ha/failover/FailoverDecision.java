package io.github.ike.ullmatcher.ha.failover;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;

/**
 * 切换决策结果。
 *
 * @param action 决策动作
 * @param reason 决策原因
 * @param candidateNodeId 待提升的候选节点；无候选时为 {@code null}
 * @param nextToken 新纪元令牌；无需切换时为当前令牌
 */
public record FailoverDecision(
        FailoverAction action,
        String reason,
        String candidateNodeId,
        FencingToken nextToken
) {
    public static FailoverDecision keep(FencingToken token, String reason) {
        return new FailoverDecision(FailoverAction.KEEP_PRIMARY, reason, null, token);
    }

    public static FailoverDecision hold(FencingToken token, String reason) {
        return new FailoverDecision(FailoverAction.HOLD, reason, null, token);
    }

    public static FailoverDecision promote(String candidateNodeId, FencingToken nextToken, String reason) {
        return new FailoverDecision(FailoverAction.PROMOTE_STANDBY, reason, candidateNodeId, nextToken);
    }
}
