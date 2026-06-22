package io.github.ike.ullmatcher.ha.failover;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.state.ReplicaState;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * 多备节点的切换决策器。
 * <p>
 * 该类只负责判定谁可以接管，不负责网络通信、选主存储或 WAL 复制实现。
 * 生产接入时应由外部控制面持久化 {@link FencingToken}，并对旧主执行 fencing。
 */
public final class QuorumFailoverController {
    /**
     * 评估主备状态并给出切换决策。
     *
     * @param primary 当前主节点
     * @param standbys 所有待命节点
     * @param policy 切换策略
     * @param nowNanos 当前单调时间
     * @return 切换决策
     */
    public FailoverDecision evaluate(ReplicaState primary, List<ReplicaState> standbys,
                                     FailoverPolicy policy, long nowNanos) {
        Objects.requireNonNull(primary, "primary");
        Objects.requireNonNull(standbys, "standbys");
        Objects.requireNonNull(policy, "policy");
        if (nowNanos < 0L) {
            throw new IllegalArgumentException("nowNanos must be non-negative");
        }
        if (primary.role() != HaRole.PRIMARY) {
            throw new IllegalArgumentException("primary replica must have PRIMARY role");
        }
        if (standbys.size() < policy.minStandbyReplicas()) {
            return FailoverDecision.hold(primary.fencingToken(), "insufficient standby replicas");
        }
        if (isPrimaryHealthy(primary, policy, nowNanos)) {
            return FailoverDecision.keep(primary.fencingToken(), "primary healthy");
        }

        ReplicaState candidate = standbys.stream()
                .filter(QuorumFailoverController::isEligibleStandby)
                .filter(replica -> lag(primary, replica) <= policy.maxPromotionLag())
                .sorted(Comparator
                        .comparingLong((ReplicaState replica) -> replica.cursor().promotionWatermark()).reversed()
                        .thenComparing(ReplicaState::nodeId))
                .findFirst()
                .orElse(null);

        if (candidate == null) {
            return FailoverDecision.hold(primary.fencingToken(), "no standby satisfies promotion lag and health checks");
        }
        return FailoverDecision.promote(
                candidate.nodeId(),
                primary.fencingToken().next(),
                "primary unavailable and standby caught up"
        );
    }

    private static boolean isPrimaryHealthy(ReplicaState primary, FailoverPolicy policy, long nowNanos) {
        if (!primary.reachable() || !primary.healthy()) {
            return false;
        }
        return nowNanos - primary.lastHeartbeatNanos() <= policy.primaryHeartbeatTimeoutNanos();
    }

    private static boolean isEligibleStandby(ReplicaState replica) {
        return (replica.role() == HaRole.STANDBY || replica.role() == HaRole.CATCHING_UP)
                && replica.reachable()
                && replica.healthy();
    }

    private static long lag(ReplicaState primary, ReplicaState standby) {
        return Math.max(0L, primary.cursor().promotionWatermark() - standby.cursor().promotionWatermark());
    }
}
