package io.github.ike.ullmatcher.ha.replication;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the default methods a transport inherits when it only implements {@code replicate}.
 * The fallbacks must preserve command order and must never silently report success for an empty
 * batch as if standbys had acked.
 */
final class CommandReplicatorTest {
    @Test
    void defaultBatchReplicatesEveryCommandInOrderAndReturnsTheLastResult() throws IOException {
        RecordingReplicator replicator = new RecordingReplicator();

        ReplicationResult result = replicator.replicateBatch(
                List.of(command(1L), command(2L), command(3L)), 42L);

        assertEquals(List.of(1L, 2L, 3L), replicator.sequences);
        assertEquals(List.of(42L, 42L, 42L), replicator.timeouts);
        assertEquals(3, result.ackedTargets(), "the last per-command result must be returned");
    }

    @Test
    void defaultBatchTreatsNullAndEmptyAsANoOpWithoutClaimingAcks() throws IOException {
        RecordingReplicator replicator = new RecordingReplicator();

        ReplicationResult empty = replicator.replicateBatch(List.of(), 1L);
        ReplicationResult nullBatch = replicator.replicateBatch(null, 1L);

        assertEquals(new ReplicationResult(0, 0, List.of(), List.of()), empty);
        assertEquals(empty, nullBatch);
        assertTrue(replicator.sequences.isEmpty());
    }

    @Test
    void defaultBatchPropagatesTheFirstFailureAndStopsReplicating() {
        RecordingReplicator replicator = new RecordingReplicator();
        replicator.failOnSequence = 2L;

        IOException error = assertThrows(IOException.class,
                () -> replicator.replicateBatch(List.of(command(1L), command(2L), command(3L)), 1L));

        assertEquals("replicate failed at 2", error.getMessage());
        assertEquals(List.of(1L), replicator.sequences, "commands after the failure must not be sent");
    }

    @Test
    void defaultAsyncBatchCompletesEagerlyOnTheCallingThread() throws Exception {
        RecordingReplicator replicator = new RecordingReplicator();

        CompletableFuture<ReplicationResult> future =
                replicator.replicateBatchAsync(List.of(command(1L), command(2L)), 1L);

        assertTrue(future.isDone(), "default async replication is synchronous");
        assertEquals(2, future.get().ackedTargets());
        assertEquals(List.of(1L, 2L), replicator.sequences);
    }

    @Test
    void defaultAsyncBatchIgnoresTheRequestedModeAndDelegatesToTheSyncPath() throws Exception {
        RecordingReplicator replicator = new RecordingReplicator();

        ReplicationResult result = replicator
                .replicateBatchAsync(List.of(command(1L)), ReplicationMode.WAIT_FOR_ALL_STANDBYS, 1L)
                .get();

        assertEquals(1, result.ackedTargets());
        assertEquals(List.of(1L), replicator.sequences);
    }

    @Test
    void defaultAsyncBatchSurfacesReplicatorFailuresSynchronously() {
        RecordingReplicator replicator = new RecordingReplicator();
        replicator.failOnSequence = 1L;

        assertThrows(IOException.class, () -> replicator.replicateBatchAsync(List.of(command(1L)), 1L));
    }

    @Test
    void defaultBatchingHintsAreConservativeAndSelfConsistent() {
        RecordingReplicator replicator = new RecordingReplicator();

        assertEquals(2_048, replicator.preferredMaxBatchSize());
        assertEquals(16, replicator.preferredInFlightBatches());
        assertEquals(TimeUnit.MICROSECONDS.toNanos(200), replicator.preferredAccumulationNanos());
        assertTrue(replicator.preferredMaxBatchSize() > 0);
        assertTrue(replicator.preferredInFlightBatches() > 0);
    }

    @Test
    void anAsyncCapableReplicatorCanOverrideTheDefaultWithoutTouchingSingleCommandReplication()
            throws ExecutionException, InterruptedException, IOException {
        ReplicationResult sentinel = new ReplicationResult(1, 1, List.of("b"), List.of());
        CommandReplicator replicator = new CommandReplicator() {
            @Override
            public ReplicationResult replicate(Command command, long timeoutNanos) {
                throw new AssertionError("single-command path must not be used");
            }

            @Override
            public CompletableFuture<ReplicationResult> replicateBatchAsync(List<Command> commands, long timeoutNanos) {
                return CompletableFuture.completedFuture(sentinel);
            }
        };

        assertSame(sentinel, replicator.replicateBatchAsync(List.of(command(1L)), 1L).get());
        assertSame(sentinel, replicator
                .replicateBatchAsync(List.of(command(1L)), ReplicationMode.WAIT_FOR_QUORUM_STANDBYS, 1L)
                .get());
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 1_000L + sequence, 2_000L + sequence, 1,
                Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
    }

    /** Replicator that only implements the single-command contract so defaults stay in play. */
    private static final class RecordingReplicator implements CommandReplicator {
        private final List<Long> sequences = new ArrayList<>();
        private final List<Long> timeouts = new ArrayList<>();
        private long failOnSequence = -1L;

        @Override
        public ReplicationResult replicate(Command command, long timeoutNanos) throws IOException {
            if (command.sequence == failOnSequence) {
                throw new IOException("replicate failed at " + command.sequence);
            }
            sequences.add(command.sequence);
            timeouts.add(timeoutNanos);
            return new ReplicationResult(sequences.size(), sequences.size(), List.of("standby"), List.of());
        }
    }
}
