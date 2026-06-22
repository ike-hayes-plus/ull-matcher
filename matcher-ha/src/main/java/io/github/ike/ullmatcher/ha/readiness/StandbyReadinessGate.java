package io.github.ike.ullmatcher.ha.readiness;

import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;

import java.util.Objects;

/**
 * standby promotion gate，显式区分“需要 catch-up”和“需要先同步快照”。
 */
public final class StandbyReadinessGate {
    public GateDecision evaluate(ReplicationCursor primary,
                                 ReplicationCursor standby,
                                 PromotionReadinessPolicy readinessPolicy,
                                 long snapshotSyncThreshold) {
        Objects.requireNonNull(primary, "primary");
        Objects.requireNonNull(standby, "standby");
        Objects.requireNonNull(readinessPolicy, "readinessPolicy");
        if (snapshotSyncThreshold < 0L) {
            throw new IllegalArgumentException("snapshotSyncThreshold must be non-negative");
        }

        PromotionReadinessReport report = new PromotionReadinessEvaluator().evaluate(primary, standby, readinessPolicy);
        if (report.ready()) {
            return new GateDecision(true, false, false, report);
        }
        boolean snapshotSyncRequired = report.snapshotLag() > snapshotSyncThreshold;
        return new GateDecision(false, true, snapshotSyncRequired, report);
    }

    public record GateDecision(
            boolean promotionReady,
            boolean catchUpRequired,
            boolean snapshotSyncRequired,
            PromotionReadinessReport report
    ) {
        public GateDecision {
            Objects.requireNonNull(report, "report");
        }
    }
}
