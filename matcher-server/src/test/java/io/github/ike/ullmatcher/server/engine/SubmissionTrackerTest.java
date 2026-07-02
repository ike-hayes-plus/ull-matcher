package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.hft.SubmitResult;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
