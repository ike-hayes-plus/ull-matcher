package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.hft.SubmitResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ReplicationCoordinatorTest {

    @Test
    void preservesCommittedOrderAcrossInFlightBatches() throws Exception {
        ControlledReplicator replicator = new ControlledReplicator();
        try (ReplicationCoordinator coordinator = new ReplicationCoordinator("node-a")) {
            coordinator.configure(replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TimeUnit.SECONDS.toNanos(1));

            List<TrackedItem> firstBatch = trackedRange(1L, 2_048);
            List<TrackedItem> secondBatch = trackedItems(3_000L);
            coordinator.onLocalAcceptedBatch(prepared(firstBatch), tracked(firstBatch));
            coordinator.onLocalAcceptedBatch(prepared(secondBatch), tracked(secondBatch));

            replicator.awaitInvocations(2);
            replicator.futureAt(1).complete(ackedResult());

            SubmissionView outOfOrderSnapshot = secondBatch.getFirst().handle.awaitCommitted(25);
            assertEquals(SubmissionPhase.REPLICATION_PENDING, outOfOrderSnapshot.phase());

            replicator.futureAt(0).complete(ackedResult());

            assertEquals(SubmissionPhase.COMMITTED, firstBatch.getFirst().handle.awaitCommitted(1_000).phase());
            assertEquals(SubmissionPhase.COMMITTED, secondBatch.getFirst().handle.awaitCommitted(1_000).phase());
        }
    }

    @Test
    void closeFailsPendingInFlightBatch() throws Exception {
        ControlledReplicator replicator = new ControlledReplicator();
        ReplicationCoordinator coordinator = new ReplicationCoordinator("node-a");
        coordinator.configure(replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TimeUnit.SECONDS.toNanos(1));

        TrackedItem item = trackedItem(10L);
        coordinator.onLocalAcceptedBatch(prepared(List.of(item)), tracked(List.of(item)));
        replicator.awaitInvocations(1);

        coordinator.close();

        SubmissionView view = item.handle.awaitCommitted(1_000);
        assertEquals(SubmissionPhase.FAILED, view.phase());
        assertTrue(view.lastError().contains("closed"));
    }

    @Test
    void retriesWhenReplicationResultDoesNotSatisfyMode() throws Exception {
        ControlledReplicator replicator = new ControlledReplicator();
        try (ReplicationCoordinator coordinator = new ReplicationCoordinator("node-a")) {
            coordinator.configure(replicator, ReplicationMode.WAIT_FOR_ALL_STANDBYS, TimeUnit.SECONDS.toNanos(1));

            TrackedItem item = trackedItem(20L);
            coordinator.onLocalAcceptedBatch(prepared(List.of(item)), tracked(List.of(item)));

            replicator.awaitInvocations(1);
            replicator.futureAt(0).complete(new ReplicationResult(2, 1, List.of("standby-a"), List.of("standby-b")));
            replicator.awaitInvocations(2);
            SubmissionView retrying = item.handle.awaitCommitted(250);
            assertEquals(SubmissionPhase.REPLICATION_PENDING, retrying.phase());
            assertEquals(1L, retrying.retryCount());
            assertTrue(retrying.lastError().contains("did not satisfy"));

            replicator.futureAt(1).complete(new ReplicationResult(2, 2, List.of("standby-a", "standby-b"), List.of()));
            assertEquals(SubmissionPhase.COMMITTED, item.handle.awaitCommitted(1_000).phase());
        }
    }

    @Test
    void retriesAfterIoExceptionAndPreservesCommittedOrder() throws Exception {
        ControlledReplicator replicator = new ControlledReplicator();
        try (ReplicationCoordinator coordinator = new ReplicationCoordinator("node-a")) {
            coordinator.configure(replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TimeUnit.SECONDS.toNanos(1));

            List<TrackedItem> firstBatch = trackedRange(30L, 2_048);
            TrackedItem first = firstBatch.getFirst();
            TrackedItem second = trackedItem(3_100L);
            coordinator.onLocalAcceptedBatch(prepared(firstBatch), tracked(firstBatch));
            coordinator.onLocalAcceptedBatch(prepared(List.of(second)), tracked(List.of(second)));

            replicator.awaitInvocations(2);
            replicator.futureAt(0).completeExceptionally(new IOException("transient replication failure"));
            replicator.futureAt(1).complete(ackedResult());

            SubmissionView retrying = first.handle.awaitCommitted(250);
            assertEquals(SubmissionPhase.REPLICATION_PENDING, retrying.phase());
            assertEquals(1L, retrying.retryCount());
            assertTrue(retrying.lastError().contains("transient replication failure"));
            assertEquals(SubmissionPhase.REPLICATION_PENDING, second.handle.awaitCommitted(250).phase());

            replicator.awaitInvocations(3);
            replicator.futureAt(2).complete(ackedResult());

            assertEquals(SubmissionPhase.COMMITTED, first.handle.awaitCommitted(1_000).phase());
            assertEquals(SubmissionPhase.COMMITTED, second.handle.awaitCommitted(1_000).phase());
        }
    }

    private static List<SubmissionRequest.PreparedSubmission> prepared(List<TrackedItem> items) {
        ArrayList<SubmissionRequest.PreparedSubmission> prepared = new ArrayList<>(items.size());
        for (TrackedItem item : items) {
            prepared.add(item.prepared);
        }
        return prepared;
    }

    private static List<SubmissionTracker.TrackedSubmission> tracked(List<TrackedItem> items) {
        ArrayList<SubmissionTracker.TrackedSubmission> tracked = new ArrayList<>(items.size());
        for (TrackedItem item : items) {
            tracked.add(item.tracked);
        }
        return tracked;
    }

    private static List<TrackedItem> trackedItems(long... sequences) {
        ArrayList<TrackedItem> items = new ArrayList<>(sequences.length);
        for (long sequence : sequences) {
            items.add(trackedItem(sequence));
        }
        return items;
    }

    private static List<TrackedItem> trackedRange(long startInclusive, int count) {
        ArrayList<TrackedItem> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            items.add(trackedItem(startInclusive + i));
        }
        return items;
    }

    private static TrackedItem trackedItem(long sequence) {
        SubmissionTracker tracker = new SubmissionTracker(new OrderStateTracker(16));
        SubmissionTracker.Registration registration = tracker.register("NEW_ORDER", "id-" + sequence, 1L, 10_000L + sequence);
        SubmissionTracker.SubmissionHandle handle = tracker.handle(registration.trackedSubmission());
        registration.trackedSubmission().markLocalOutcome(
                sequence,
                SubmitResult.ACCEPTED,
                System.currentTimeMillis(),
                true,
                1,
                1
        );
        Command command = Command.newOrder(sequence, 10_000L + sequence, 1L, 1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
        SubmissionRequest.PreparedSubmission prepared = new SubmissionRequest.PreparedSubmission(command, sequence, () -> {});
        return new TrackedItem(prepared, registration.trackedSubmission(), handle);
    }

    private static ReplicationResult ackedResult() {
        return new ReplicationResult(1, 1, List.of("standby-a"), List.of());
    }

    private record TrackedItem(SubmissionRequest.PreparedSubmission prepared,
                               SubmissionTracker.TrackedSubmission tracked,
                               SubmissionTracker.SubmissionHandle handle) {
    }

    private static final class ControlledReplicator implements CommandReplicator {
        private final ArrayDeque<CompletableFuture<ReplicationResult>> futures = new ArrayDeque<>();
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public ReplicationResult replicate(Command command, long timeoutNanos) {
            throw new UnsupportedOperationException("single-command path is not used in this test");
        }

        @Override
        public synchronized CompletableFuture<ReplicationResult> replicateBatchAsync(List<Command> commands, long timeoutNanos) {
            CompletableFuture<ReplicationResult> future = new CompletableFuture<>();
            futures.addLast(future);
            invocations.incrementAndGet();
            return future;
        }

        CompletableFuture<ReplicationResult> futureAt(int index) {
            return futures.stream().skip(index).findFirst().orElseThrow();
        }

        void awaitInvocations(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (invocations.get() < expected && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(expected, invocations.get());
        }
    }
}
