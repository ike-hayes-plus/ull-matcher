package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.hft.SubmitResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SubmissionTrackerTest {
    @Test
    void finalizedSubmissionsAreEvictedWhenTrackedLimitIsExceeded() {
        SubmissionTracker tracker = new SubmissionTracker(
                new OrderStateTracker(16),
                Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
                1
        );

        SubmissionTracker.Registration first = tracker.register("NEW_ORDER", "key-1", 1L, 101L, 11L);
        first.trackedSubmission().markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_001L, false, 0, 0);
        assertEquals(1L, tracker.metricsSnapshot().trackedCount());

        SubmissionTracker.Registration second = tracker.register("NEW_ORDER", "key-2", 1L, 102L, 12L);
        assertNull(tracker.findByIdempotencyKey("key-1"));

        second.trackedSubmission().markLocalOutcome(2L, SubmitResult.ACCEPTED, 1_002L, false, 0, 0);
        assertEquals(1L, tracker.metricsSnapshot().trackedCount());
        assertEquals(1L, tracker.metricsSnapshot().committedCount());
        assertEquals(2L, tracker.metricsSnapshot().committedTotal());
        assertEquals(0L, tracker.metricsSnapshot().pendingCount());
    }

    @Test
    void activeSubmissionsAreRejectedWhenTrackedLimitIsExceeded() {
        SubmissionTracker tracker = new SubmissionTracker(
                new OrderStateTracker(16),
                Clock.fixed(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
                1
        );

        SubmissionTracker.Registration first = tracker.register("NEW_ORDER", "key-1", 1L, 101L, 11L);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> tracker.register("NEW_ORDER", "key-2", 1L, 102L, 12L));
        assertEquals("too many tracked submissions", error.getMessage());
        assertEquals(1L, tracker.metricsSnapshot().trackedCount());
        assertEquals(1L, tracker.metricsSnapshot().pendingCount());
        assertNull(tracker.findByIdempotencyKey("key-2"));
        assertEquals(first.trackedSubmission().submissionId(), tracker.findByIdempotencyKey("key-1").submissionId());
    }

    @Test
    void reusedIdempotencyKeyWithDifferentRequestIsRejected() {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));

        SubmissionTracker.Registration first = tracker.register("NEW_ORDER", "same-key", 1L, 101L, 11L);
        SubmissionTracker.Registration same = tracker.register("NEW_ORDER", "same-key", 1L, 101L, 11L);

        assertTrue(same.existing());
        assertEquals(first.trackedSubmission().submissionId(), same.trackedSubmission().submissionId());

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> tracker.register("NEW_ORDER", "same-key", 1L, 101L, 12L));
        assertEquals("idempotency key reused with different request", error.getMessage());
    }

    @Test
    void reusedIdempotencyKeyComparesExactFingerprintFieldsEvenWhenHashCollides() {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.RequestFingerprint buyAt100 =
                new SubmissionTracker.RequestFingerprint(99L, (byte) 'B', (byte) 'L', (byte) 'G', 100L, 1L, Long.MIN_VALUE);
        SubmissionTracker.RequestFingerprint buyAt101 =
                new SubmissionTracker.RequestFingerprint(99L, (byte) 'B', (byte) 'L', (byte) 'G', 101L, 1L, Long.MIN_VALUE);

        SubmissionTracker.Registration first = tracker.register("NEW_ORDER", "hash-collision", 1L, 101L, buyAt100);
        SubmissionTracker.Registration same = tracker.register("NEW_ORDER", "hash-collision", 1L, 101L, buyAt100);

        assertTrue(same.existing());
        assertEquals(first.trackedSubmission().submissionId(), same.trackedSubmission().submissionId());

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> tracker.register("NEW_ORDER", "hash-collision", 1L, 101L, buyAt101));
        assertEquals("idempotency key reused with different request", error.getMessage());
    }

    @Test
    void replicationRetryBeforeLocalOutcomeIsIgnored() {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 101L, 11L);
        registration.trackedSubmission().markReplicationRetry(new IOException("early"), 1_000L);
        assertEquals(0L, tracker.metricsSnapshot().retryingCount());
        assertEquals(SubmissionPhase.RECEIVED, registration.trackedSubmission().snapshot(null).phase());
    }

    @Test
    void replicationCommittedAfterLocalFailureIsIgnored() {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 101L, 11L);
        var tracked = registration.trackedSubmission();
        tracked.markLocalOutcome(1L, SubmitResult.MATCHER_NOT_RUNNING, 1_000L, true, 1, 1);
        tracked.markReplicationCommitted(
                new ReplicationResult(1, 1, List.of("node-b"), List.of()),
                ReplicationMode.WAIT_FOR_ANY_STANDBY,
                1_001L);
        assertEquals(SubmissionPhase.FAILED, tracked.snapshot(null).phase());
    }

    @Test
    void localRejectSkipsReplicationWait() throws IOException {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 101L, 11L);
        registration.trackedSubmission().markLocalOutcome(1L, SubmitResult.MATCHER_NOT_RUNNING, 1_000L, true, 1, 1);
        SubmissionReceipt receipt = tracker.handle(registration.trackedSubmission()).awaitCommittedReceipt(100L);
        assertEquals(SubmissionPhase.FAILED, receipt.phase());
        assertFalse(receipt.replicationCommitted());
    }

    @Test
    void replicationRetryAndCommitUpdateReceipt() throws IOException {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 101L, 11L);
        var tracked = registration.trackedSubmission();
        tracked.markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, true, 1, 1);
        tracked.markReplicationRetry(new IOException("retry"), 1_001L);
        tracked.markReplicationObservation(new ReplicationResult(2, 1, List.of("node-b"), List.of()), ReplicationMode.WAIT_FOR_QUORUM_STANDBYS, 1_002L);
        ReplicationResult acked = new ReplicationResult(2, 2, List.of("node-b", "node-c"), List.of());
        tracked.markReplicationCommitted(acked, ReplicationMode.WAIT_FOR_QUORUM_STANDBYS, 1_003L);
        SubmissionReceipt receipt = tracker.handle(tracked).awaitCommittedReceipt(100L);
        assertTrue(receipt.replicationCommitted());
        assertEquals(1L, receipt.retryCount());
    }

    @Test
    void replicationUpdatesAfterFinalizeAreIgnored() {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 101L, 11L);
        var tracked = registration.trackedSubmission();
        tracked.markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, false, 0, 0);
        tracked.markReplicationCommitted(
                new ReplicationResult(1, 1, List.of("node-b"), List.of()),
                ReplicationMode.WAIT_FOR_ANY_STANDBY,
                1_001L);
        tracked.markReplicationRetry(new IOException("late"), 1_002L);
        tracked.markReplicationObservation(
                new ReplicationResult(1, 0, List.of(), List.of("node-b")),
                ReplicationMode.WAIT_FOR_ANY_STANDBY,
                1_003L);
        assertEquals(SubmissionPhase.COMMITTED, tracked.snapshot(null).phase());
    }

    @Test
    void duplicateLocalOutcomeIsIgnored() {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 101L, 11L);
        var tracked = registration.trackedSubmission();
        tracked.markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, false, 0, 0);
        tracked.markLocalOutcome(99L, SubmitResult.MATCHER_NOT_RUNNING, 2_000L, true, 1, 1);
        assertEquals(1L, tracked.snapshot(null).sequence());
        assertEquals(SubmissionPhase.COMMITTED, tracked.snapshot(null).phase());
    }

    @Test
    void closedFailureFinalizesPendingSubmission() throws IOException {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 101L, 11L);
        var tracked = registration.trackedSubmission();
        tracked.markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, true, 1, 1);
        tracked.markClosedFailure("shutdown", 1_004L);
        SubmissionReceipt receipt = tracker.handle(tracked).awaitCommittedReceipt(100L);
        assertEquals(SubmissionPhase.FAILED, receipt.phase());
        assertEquals("shutdown", receipt.lastError());
    }
}
