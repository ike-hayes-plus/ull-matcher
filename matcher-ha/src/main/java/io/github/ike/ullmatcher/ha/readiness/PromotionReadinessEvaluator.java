package io.github.ike.ullmatcher.ha.readiness;

import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;

import java.util.Objects;

/**
 * standby promotion readiness 评估器。
 */
public final class PromotionReadinessEvaluator {
    public PromotionReadinessReport evaluate(ReplicationCursor primary,
                                             ReplicationCursor standby,
                                             PromotionReadinessPolicy policy) {
        Objects.requireNonNull(primary, "primary");
        Objects.requireNonNull(standby, "standby");
        Objects.requireNonNull(policy, "policy");

        long receivedLag = Math.max(0L, primary.lastReceivedSequence() - standby.lastReceivedSequence());
        long durableLag = Math.max(0L, primary.lastDurableSequence() - standby.lastDurableSequence());
        long appliedLag = Math.max(0L, primary.lastAppliedSequence() - standby.lastAppliedSequence());
        long snapshotLag = Math.max(0L, primary.snapshotSequence() - standby.snapshotSequence());

        if (receivedLag > policy.maxReceivedLag()) {
            return new PromotionReadinessReport(false, receivedLag, durableLag, appliedLag, snapshotLag, "received lag exceeds threshold");
        }
        if (durableLag > policy.maxDurableLag()) {
            return new PromotionReadinessReport(false, receivedLag, durableLag, appliedLag, snapshotLag, "durable lag exceeds threshold");
        }
        if (appliedLag > policy.maxAppliedLag()) {
            return new PromotionReadinessReport(false, receivedLag, durableLag, appliedLag, snapshotLag, "applied lag exceeds threshold");
        }
        if (snapshotLag > policy.maxSnapshotLag()) {
            return new PromotionReadinessReport(false, receivedLag, durableLag, appliedLag, snapshotLag, "snapshot lag exceeds threshold");
        }
        return new PromotionReadinessReport(true, receivedLag, durableLag, appliedLag, snapshotLag, "standby is ready for promotion");
    }
}
