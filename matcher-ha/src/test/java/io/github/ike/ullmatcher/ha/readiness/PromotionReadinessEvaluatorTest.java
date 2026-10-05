package io.github.ike.ullmatcher.ha.readiness;

import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Promotion readiness is the last gate before a standby starts serving clients, so every rejection
 * reason must be attributable to a specific lag dimension and the lag numbers must never be negative.
 */
final class PromotionReadinessEvaluatorTest {
    private final PromotionReadinessEvaluator evaluator = new PromotionReadinessEvaluator();

    @Test
    void standbyWithIdenticalCursorIsReadyUnderStrictPolicy() {
        ReplicationCursor cursor = new ReplicationCursor(100L, 100L, 100L, 90L);

        PromotionReadinessReport report = evaluator.evaluate(cursor, cursor, PromotionReadinessPolicy.strict());

        assertTrue(report.ready());
        assertEquals(0L, report.receivedLag());
        assertEquals(0L, report.durableLag());
        assertEquals(0L, report.appliedLag());
        assertEquals(0L, report.snapshotLag());
        assertEquals("standby is ready for promotion", report.reason());
    }

    @Test
    void lagWithinPolicyThresholdsStillCountsAsReady() {
        PromotionReadinessReport report = evaluator.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 100L),
                new ReplicationCursor(98L, 97L, 96L, 95L),
                new PromotionReadinessPolicy(2L, 3L, 4L, 5L));

        assertTrue(report.ready());
        assertEquals(2L, report.receivedLag());
        assertEquals(3L, report.durableLag());
        assertEquals(4L, report.appliedLag());
        assertEquals(5L, report.snapshotLag());
    }

    @Test
    void receivedLagOneBeyondThresholdIsRejected() {
        PromotionReadinessReport report = evaluator.evaluate(
                new ReplicationCursor(100L, 0L, 0L, 0L),
                new ReplicationCursor(97L, 0L, 0L, 0L),
                new PromotionReadinessPolicy(2L, 100L, 100L, 100L));

        assertFalse(report.ready());
        assertEquals(3L, report.receivedLag());
        assertEquals("received lag exceeds threshold", report.reason());
    }

    @Test
    void durableLagIsReportedWhenReceivedLagIsAcceptable() {
        PromotionReadinessReport report = evaluator.evaluate(
                new ReplicationCursor(100L, 100L, 0L, 0L),
                new ReplicationCursor(100L, 90L, 0L, 0L),
                new PromotionReadinessPolicy(0L, 5L, 100L, 100L));

        assertFalse(report.ready());
        assertEquals(0L, report.receivedLag());
        assertEquals(10L, report.durableLag());
        assertEquals("durable lag exceeds threshold", report.reason());
    }

    @Test
    void appliedLagIsReportedWhenReceivedAndDurableAreAcceptable() {
        PromotionReadinessReport report = evaluator.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 0L),
                new ReplicationCursor(100L, 100L, 80L, 0L),
                new PromotionReadinessPolicy(0L, 0L, 10L, 100L));

        assertFalse(report.ready());
        assertEquals(20L, report.appliedLag());
        assertEquals("applied lag exceeds threshold", report.reason());
    }

    @Test
    void snapshotLagIsReportedLast() {
        PromotionReadinessReport report = evaluator.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 100L),
                new ReplicationCursor(100L, 100L, 100L, 10L),
                new PromotionReadinessPolicy(0L, 0L, 0L, 50L));

        assertFalse(report.ready());
        assertEquals(90L, report.snapshotLag());
        assertEquals("snapshot lag exceeds threshold", report.reason());
    }

    @Test
    void receivedLagIsReportedBeforeOtherDimensionsWhenEverythingIsBehind() {
        PromotionReadinessReport report = evaluator.evaluate(
                new ReplicationCursor(100L, 100L, 100L, 100L),
                new ReplicationCursor(0L, 0L, 0L, 0L),
                PromotionReadinessPolicy.strict());

        assertEquals("received lag exceeds threshold", report.reason());
        assertEquals(100L, report.receivedLag());
        assertEquals(100L, report.durableLag());
        assertEquals(100L, report.appliedLag());
        assertEquals(100L, report.snapshotLag());
    }

    @Test
    void standbyAheadOfPrimaryClampsLagToZeroInsteadOfReportingNegativeLag() {
        PromotionReadinessReport report = evaluator.evaluate(
                new ReplicationCursor(10L, 10L, 10L, 10L),
                new ReplicationCursor(50L, 40L, 30L, 20L),
                PromotionReadinessPolicy.strict());

        assertTrue(report.ready(), "a standby ahead of a stale primary must not be blocked");
        assertEquals(0L, report.receivedLag());
        assertEquals(0L, report.durableLag());
        assertEquals(0L, report.appliedLag());
        assertEquals(0L, report.snapshotLag());
    }

    @Test
    void nullArgumentsAreRejected() {
        ReplicationCursor cursor = new ReplicationCursor(1L, 1L, 1L, 1L);
        PromotionReadinessPolicy policy = PromotionReadinessPolicy.strict();

        assertThrows(NullPointerException.class, () -> evaluator.evaluate(null, cursor, policy));
        assertThrows(NullPointerException.class, () -> evaluator.evaluate(cursor, null, policy));
        assertThrows(NullPointerException.class, () -> evaluator.evaluate(cursor, cursor, null));
    }
}
