package io.github.ike.ullmatcher.ha.readiness;

import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The gate turns a readiness verdict into an operational instruction: promote now, stream the WAL
 * tail, or fetch a full snapshot first. Picking the wrong one either stalls failover or replays a
 * gap, so each branch is pinned down here.
 */
final class StandbyReadinessGateTest {
    private final StandbyReadinessGate gate = new StandbyReadinessGate();

    @Test
    void readyStandbyNeedsNeitherCatchUpNorSnapshot() {
        ReplicationCursor cursor = new ReplicationCursor(50L, 50L, 50L, 50L);

        StandbyReadinessGate.GateDecision decision =
                gate.evaluate(cursor, cursor, PromotionReadinessPolicy.strict(), 0L);

        assertTrue(decision.promotionReady());
        assertFalse(decision.catchUpRequired());
        assertFalse(decision.snapshotSyncRequired());
        assertTrue(decision.report().ready());
    }

    @Test
    void smallWalGapAsksForCatchUpWithoutASnapshot() {
        StandbyReadinessGate.GateDecision decision = gate.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 100L),
                new ReplicationCursor(90L, 90L, 90L, 100L),
                PromotionReadinessPolicy.strict(),
                1_000L);

        assertFalse(decision.promotionReady());
        assertTrue(decision.catchUpRequired());
        assertFalse(decision.snapshotSyncRequired(), "snapshot is aligned, so WAL catch-up is enough");
        assertEquals("received lag exceeds threshold", decision.report().reason());
    }

    @Test
    void snapshotLagBeyondThresholdEscalatesToASnapshotSync() {
        StandbyReadinessGate.GateDecision decision = gate.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 100L),
                new ReplicationCursor(10L, 10L, 10L, 10L),
                PromotionReadinessPolicy.strict(),
                50L);

        assertFalse(decision.promotionReady());
        assertTrue(decision.catchUpRequired());
        assertTrue(decision.snapshotSyncRequired());
        assertEquals(90L, decision.report().snapshotLag());
    }

    @Test
    void snapshotLagExactlyAtThresholdStaysOnTheCatchUpPath() {
        StandbyReadinessGate.GateDecision atThreshold = gate.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 100L),
                new ReplicationCursor(100L, 100L, 100L, 50L),
                PromotionReadinessPolicy.strict(),
                50L);
        StandbyReadinessGate.GateDecision oneBeyond = gate.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 100L),
                new ReplicationCursor(100L, 100L, 100L, 49L),
                PromotionReadinessPolicy.strict(),
                50L);

        assertFalse(atThreshold.snapshotSyncRequired());
        assertTrue(oneBeyond.snapshotSyncRequired());
    }

    @Test
    void aReadyStandbyIsNotForcedIntoASnapshotEvenWithAZeroThreshold() {
        ReplicationCursor primary = new ReplicationCursor(10L, 10L, 10L, 10L);
        ReplicationCursor standby = new ReplicationCursor(10L, 10L, 10L, 5L);

        StandbyReadinessGate.GateDecision decision = gate.evaluate(
                primary, standby, new PromotionReadinessPolicy(0L, 0L, 0L, 5L), 0L);

        assertTrue(decision.promotionReady());
        assertFalse(decision.snapshotSyncRequired());
    }

    @Test
    void negativeSnapshotThresholdIsRejected() {
        ReplicationCursor cursor = new ReplicationCursor(1L, 1L, 1L, 1L);

        assertThrows(IllegalArgumentException.class,
                () -> gate.evaluate(cursor, cursor, PromotionReadinessPolicy.strict(), -1L));
    }

    @Test
    void nullArgumentsAreRejected() {
        ReplicationCursor cursor = new ReplicationCursor(1L, 1L, 1L, 1L);
        PromotionReadinessPolicy policy = PromotionReadinessPolicy.strict();

        assertThrows(NullPointerException.class, () -> gate.evaluate(null, cursor, policy, 0L));
        assertThrows(NullPointerException.class, () -> gate.evaluate(cursor, null, policy, 0L));
        assertThrows(NullPointerException.class, () -> gate.evaluate(cursor, cursor, null, 0L));
    }

    @Test
    void gateDecisionRequiresAReport() {
        assertThrows(NullPointerException.class,
                () -> new StandbyReadinessGate.GateDecision(false, true, true, null));
    }

    @Test
    void gateDecisionsWithTheSameVerdictAreEqual() {
        PromotionReadinessReport report =
                new PromotionReadinessReport(true, 0L, 0L, 0L, 0L, "standby is ready for promotion");

        StandbyReadinessGate.GateDecision first = new StandbyReadinessGate.GateDecision(true, false, false, report);
        StandbyReadinessGate.GateDecision second = new StandbyReadinessGate.GateDecision(true, false, false, report);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertTrue(first.toString().contains("promotionReady=true"));
    }
}
