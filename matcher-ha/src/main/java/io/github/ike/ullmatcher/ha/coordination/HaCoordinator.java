package io.github.ike.ullmatcher.ha.coordination;

import io.github.ike.ullmatcher.ha.failover.FailoverAction;
import io.github.ike.ullmatcher.ha.failover.FailoverDecision;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.failover.QuorumFailoverController;
import io.github.ike.ullmatcher.ha.state.ReplicaState;

import java.util.List;
import java.util.Objects;

/**
 * HA 控制面协调器。
 * <p>
 * 负责把租约存储、故障切换决策器和本地运行时串起来。
 */
public final class HaCoordinator {
    private final String localNodeId;
    private final HaMatchRuntime runtime;
    private final LeaseStore leaseStore;
    private final QuorumFailoverController failoverController;
    private final FailoverPolicy policy;
    private final long leaseTtlNanos;

    public HaCoordinator(String localNodeId,
                         HaMatchRuntime runtime,
                         LeaseStore leaseStore,
                         QuorumFailoverController failoverController,
                         FailoverPolicy policy,
                         long leaseTtlNanos) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.leaseStore = Objects.requireNonNull(leaseStore, "leaseStore");
        this.failoverController = Objects.requireNonNull(failoverController, "failoverController");
        this.policy = Objects.requireNonNull(policy, "policy");
        if (leaseTtlNanos <= 0L) {
            throw new IllegalArgumentException("leaseTtlNanos must be positive");
        }
        this.leaseTtlNanos = leaseTtlNanos;
    }

    /**
     * 执行一次控制面评估。
     */
    public HaTickResult tick(ReplicaState observedPrimary, List<ReplicaState> standbys, long nowNanos) {
        Objects.requireNonNull(observedPrimary, "observedPrimary");
        Objects.requireNonNull(standbys, "standbys");
        if (nowNanos < 0L) {
            throw new IllegalArgumentException("nowNanos must be non-negative");
        }

        HaRole before = runtime.role();
        if (before == HaRole.PRIMARY && !leaseStore.isHeldBy(localNodeId, runtime.fencingToken(), nowNanos)) {
            runtime.fence();
            return new HaTickResult(FailoverAction.HOLD, before, runtime.role(), false, "primary lost lease and was fenced");
        }

        FailoverDecision decision = failoverController.evaluate(observedPrimary, standbys, policy, nowNanos);
        return switch (decision.action()) {
            case KEEP_PRIMARY -> keepPrimary(nowNanos, before);
            case HOLD -> new HaTickResult(decision.action(), before, runtime.role(), false, decision.reason());
            case PROMOTE_STANDBY -> promoteIfLocal(decision, nowNanos, before);
        };
    }

    private HaTickResult keepPrimary(long nowNanos, HaRole before) {
        if (runtime.role() != HaRole.PRIMARY) {
            return new HaTickResult(FailoverAction.KEEP_PRIMARY, before, runtime.role(), false, "local node is not primary");
        }
        boolean renewed = leaseStore.tryExtend(localNodeId, runtime.fencingToken(), nowNanos, leaseTtlNanos);
        if (!renewed) {
            runtime.fence();
            return new HaTickResult(FailoverAction.HOLD, before, runtime.role(), false, "primary failed to renew lease and was fenced");
        }
        return new HaTickResult(FailoverAction.KEEP_PRIMARY, before, runtime.role(), true, "primary lease renewed");
    }

    private HaTickResult promoteIfLocal(FailoverDecision decision, long nowNanos, HaRole before) {
        if (!localNodeId.equals(decision.candidateNodeId())) {
            return new HaTickResult(decision.action(), before, runtime.role(), false, decision.reason());
        }
        boolean acquired = leaseStore.tryAcquire(localNodeId, decision.nextToken(), nowNanos, leaseTtlNanos);
        if (!acquired) {
            return new HaTickResult(FailoverAction.HOLD, before, runtime.role(), false, "failed to acquire lease for promotion");
        }
        runtime.promote(decision.nextToken());
        return new HaTickResult(FailoverAction.PROMOTE_STANDBY, before, runtime.role(), true, decision.reason());
    }
}
