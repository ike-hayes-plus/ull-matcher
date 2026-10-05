package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.hft.SubmitResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BatchReplicationAwaitTest {
    @Test
    void emptyBatchIsNoOp() throws IOException {
        BatchReplicationAwait.awaitCommittedReceipts(
                new SubmissionTracker.SubmissionHandle[0],
                1_000L,
                new SubmissionReceipt[0]);
    }

    @Test
    void singleHandleUsesDirectAwait() throws IOException {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 10L, 1L);
        registration.trackedSubmission().markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, false, 0, 0);
        SubmissionTracker.SubmissionHandle handle = tracker.handle(registration.trackedSubmission());
        SubmissionReceipt[] receipts = new SubmissionReceipt[1];
        BatchReplicationAwait.awaitCommittedReceipts(new SubmissionTracker.SubmissionHandle[]{handle}, 1_000L, receipts);
        assertTrue(receipts[0].replicationCommitted());
    }

    @Test
    void multipleHandlesAwaitWithSharedDeadline() throws IOException {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration first = tracker.register("NEW_ORDER", "k1", 1L, 11L, 1L);
        SubmissionTracker.Registration second = tracker.register("NEW_ORDER", "k2", 1L, 12L, 2L);
        first.trackedSubmission().markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, true, 1, 1);
        second.trackedSubmission().markLocalOutcome(2L, SubmitResult.ACCEPTED, 1_000L, true, 1, 1);
        ReplicationResult result = new ReplicationResult(1, 1, List.of("node-b"), List.of());
        ReplicationMode mode = ReplicationMode.WAIT_FOR_ANY_STANDBY;
        first.trackedSubmission().markReplicationCommitted(result, mode, 1_001L);
        second.trackedSubmission().markReplicationCommitted(result, mode, 1_001L);
        SubmissionTracker.SubmissionHandle[] handles = {
                tracker.handle(first.trackedSubmission()),
                tracker.handle(second.trackedSubmission())
        };
        SubmissionReceipt[] receipts = new SubmissionReceipt[2];
        BatchReplicationAwait.awaitCommittedReceipts(handles, 5_000L, receipts);
        assertEquals(11L, receipts[0].orderId());
        assertEquals(12L, receipts[1].orderId());
        assertTrue(receipts[0].replicationCommitted());
        assertTrue(receipts[1].replicationCommitted());
    }

    @Test
    void returnsPendingReceiptWhenReplicationTimesOut() throws IOException {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 10L, 1L);
        registration.trackedSubmission().markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, true, 1, 1);
        SubmissionTracker.SubmissionHandle handle = tracker.handle(registration.trackedSubmission());
        SubmissionReceipt[] receipts = new SubmissionReceipt[1];
        BatchReplicationAwait.awaitCommittedReceipts(new SubmissionTracker.SubmissionHandle[]{handle}, 1L, receipts);
        assertEquals(SubmissionPhase.REPLICATION_PENDING, receipts[0].phase());
        assertEquals(false, receipts[0].replicationCommitted());
    }

    @Test
    void lateCommitUnblocksAwait() throws Exception {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "k1", 1L, 10L, 1L);
        registration.trackedSubmission().markLocalOutcome(1L, SubmitResult.ACCEPTED, 1_000L, true, 1, 1);
        SubmissionTracker.SubmissionHandle handle = tracker.handle(registration.trackedSubmission());
        SubmissionReceipt[] receipts = new SubmissionReceipt[1];
        CompletableFuture<Void> done = CompletableFuture.runAsync(() -> {
            try {
                BatchReplicationAwait.awaitCommittedReceipts(new SubmissionTracker.SubmissionHandle[]{handle}, 5_000L, receipts);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(30L);
        ReplicationResult result = new ReplicationResult(1, 1, List.of("node-b"), List.of());
        registration.trackedSubmission().markReplicationCommitted(result, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1_002L);
        done.get(2, TimeUnit.SECONDS);
        assertTrue(receipts[0].replicationCommitted());
    }
}
