package io.github.ike.ullmatcher.ha.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code withPolicy} and {@code withReconciliation} rewrite disjoint slices of the same snapshot.
 * If either one clobbered the other's fields, operators would see a healthy transport while
 * reconciliation was reporting gaps, so the non-overlap is asserted explicitly.
 */
final class TransportMetricsSnapshotTest {
    @Test
    void noneStartsFromZeroCountersWithReconciliationDisabled() {
        TransportMetricsSnapshot snapshot = TransportMetricsSnapshot.none("GRPC");

        assertEquals("GRPC", snapshot.transportType());
        assertEquals(0L, snapshot.previewPublishedCommands());
        assertEquals(0L, snapshot.previewPublishedBytes());
        assertEquals(0L, snapshot.previewPublishFailures());
        assertEquals(0L, snapshot.previewReceivedCommands());
        assertEquals(0L, snapshot.previewReceivedBytes());
        assertEquals(0L, snapshot.snapshotRequests());
        assertEquals(0L, snapshot.snapshotRequestFailures());
        assertEquals(0L, snapshot.snapshotBytesSent());
        assertEquals(0L, snapshot.snapshotBytesReceived());
        assertEquals(0L, snapshot.controlRequests());
        assertEquals(0L, snapshot.controlRequestFailures());
        assertEquals(0L, snapshot.authoritativeLastReceivedSequence());
        assertEquals(0L, snapshot.previewLastReceivedSequence());
        assertEquals(0L, snapshot.previewGapCount());
        assertEquals(0L, snapshot.previewOutOfOrderCount());
        assertEquals("DISABLED", snapshot.reconciliationStatus());
        assertEquals("sequence reconciliation is disabled for this transport mode",
                snapshot.reconciliationConclusion());
        assertEquals("STABLE", snapshot.policyStatus());
        assertEquals("transport policy is stable", snapshot.policyConclusion());
    }

    @Test
    void withPolicyReplacesOnlyThePolicyVerdict() {
        TransportMetricsSnapshot base = populated();

        TransportMetricsSnapshot updated = base.withPolicy("BLOCKED", "transport change window is closed");

        assertEquals("BLOCKED", updated.policyStatus());
        assertEquals("transport change window is closed", updated.policyConclusion());
        assertEquals(base.withPolicy(base.policyStatus(), base.policyConclusion()), base,
                "rewriting the policy with its own values must be a no-op");
        assertEquals(base.reconciliationStatus(), updated.reconciliationStatus());
        assertEquals(base.reconciliationConclusion(), updated.reconciliationConclusion());
        assertEquals(base.authoritativeLastReceivedSequence(), updated.authoritativeLastReceivedSequence());
        assertEquals(base.previewLastReceivedSequence(), updated.previewLastReceivedSequence());
        assertEquals(base.previewGapCount(), updated.previewGapCount());
        assertEquals(base.previewOutOfOrderCount(), updated.previewOutOfOrderCount());
        assertEquals(base.previewPublishedCommands(), updated.previewPublishedCommands());
        assertEquals(base.controlRequestFailures(), updated.controlRequestFailures());
    }

    @Test
    void withReconciliationReplacesOnlyTheReconciliationView() {
        TransportMetricsSnapshot base = populated();

        TransportMetricsSnapshot updated = base.withReconciliation(
                900L, 880L, 2L, 1L, "DIVERGED", "preview stream is behind the authoritative stream");

        assertEquals(900L, updated.authoritativeLastReceivedSequence());
        assertEquals(880L, updated.previewLastReceivedSequence());
        assertEquals(2L, updated.previewGapCount());
        assertEquals(1L, updated.previewOutOfOrderCount());
        assertEquals("DIVERGED", updated.reconciliationStatus());
        assertEquals("preview stream is behind the authoritative stream", updated.reconciliationConclusion());
        assertEquals(base.policyStatus(), updated.policyStatus());
        assertEquals(base.policyConclusion(), updated.policyConclusion());
        assertEquals(base.transportType(), updated.transportType());
        assertEquals(base.previewPublishedBytes(), updated.previewPublishedBytes());
        assertEquals(base.snapshotBytesReceived(), updated.snapshotBytesReceived());
    }

    @Test
    void policyAndReconciliationUpdatesComposeInEitherOrder() {
        TransportMetricsSnapshot base = populated();

        TransportMetricsSnapshot policyThenReconciliation = base
                .withPolicy("BLOCKED", "blocked")
                .withReconciliation(1L, 2L, 3L, 4L, "DIVERGED", "diverged");
        TransportMetricsSnapshot reconciliationThenPolicy = base
                .withReconciliation(1L, 2L, 3L, 4L, "DIVERGED", "diverged")
                .withPolicy("BLOCKED", "blocked");

        assertEquals(policyThenReconciliation, reconciliationThenPolicy);
        assertEquals(policyThenReconciliation.hashCode(), reconciliationThenPolicy.hashCode());
    }

    @Test
    void updatesReturnANewSnapshotRatherThanMutatingTheOriginal() {
        TransportMetricsSnapshot base = TransportMetricsSnapshot.none("AERON");

        base.withPolicy("BLOCKED", "blocked");
        base.withReconciliation(1L, 1L, 0L, 0L, "STABLE", "stable");

        assertEquals("STABLE", base.policyStatus());
        assertEquals("DISABLED", base.reconciliationStatus());
        assertFalse(base.toString().contains("BLOCKED"));
        assertTrue(base.toString().contains("transportType=AERON"));
    }

    private static TransportMetricsSnapshot populated() {
        return new TransportMetricsSnapshot(
                "AERON_PREVIEW",
                11L, 12L, 13L,
                14L, 15L,
                16L, 17L, 18L, 19L,
                20L, 21L,
                1_000L, 999L, 1L, 2L,
                "STABLE", "streams agree",
                "STABLE", "transport policy is stable");
    }
}
