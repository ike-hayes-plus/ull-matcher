package io.github.ike.ullmatcher.ha.replication;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The replicated writer is the commit boundary on the primary: a command must hit the local WAL
 * first, and the append must only be reported as successful once the replication mode is satisfied.
 */
final class ReplicatedWalWriterTest {
    private static final long TIMEOUT_NANOS = 5_000L;

    @Test
    void appendWritesLocalWalBeforeReplicatingAndPassesTheConfiguredBudget() throws IOException {
        RecordingWal localWal = new RecordingWal();
        StubReplicator replicator = new StubReplicator(new ReplicationResult(1, 1, List.of("b"), List.of()));
        ReplicatedWalWriter writer = new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TIMEOUT_NANOS);

        writer.append(command(1L));

        assertEquals(List.of("append:1"), localWal.events);
        assertEquals(List.of(1L), replicator.sequences);
        assertEquals(List.of(TIMEOUT_NANOS), replicator.timeouts);
    }

    @Test
    void appendFailsWhenTheReplicationModeIsNotSatisfied() {
        RecordingWal localWal = new RecordingWal();
        StubReplicator replicator = new StubReplicator(new ReplicationResult(2, 1, List.of("b"), List.of("c")));
        ReplicatedWalWriter writer = new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.WAIT_FOR_ALL_STANDBYS, TIMEOUT_NANOS);

        IOException error = assertThrows(IOException.class, () -> writer.append(command(1L)));

        assertTrue(error.getMessage().startsWith("replication did not satisfy mode WAIT_FOR_ALL_STANDBYS"));
        assertTrue(error.getMessage().contains("ackedTargets=1"), "the result must be in the message: " + error);
        assertEquals(List.of("append:1"), localWal.events,
                "the local WAL append is not rolled back; the caller must treat the command as uncommitted");
    }

    @Test
    void localOnlyModeCommitsWithoutAnyStandbyAck() throws IOException {
        RecordingWal localWal = new RecordingWal();
        StubReplicator replicator = new StubReplicator(new ReplicationResult(3, 0, List.of(), List.of("b", "c", "d")));
        ReplicatedWalWriter writer = new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.LOCAL_ONLY, 0L);

        writer.append(command(1L));

        assertEquals(List.of("append:1"), localWal.events);
        assertEquals(List.of(0L), replicator.timeouts, "a zero budget must be passed through unchanged");
    }

    @Test
    void aFailingLocalWalShortCircuitsReplication() {
        RecordingWal localWal = new RecordingWal();
        localWal.failOnAppend = true;
        StubReplicator replicator = new StubReplicator(new ReplicationResult(1, 1, List.of("b"), List.of()));
        ReplicatedWalWriter writer = new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TIMEOUT_NANOS);

        assertThrows(IOException.class, () -> writer.append(command(1L)));

        assertTrue(replicator.sequences.isEmpty(), "nothing may be replicated when the local WAL rejected the write");
    }

    @Test
    void replicatorFailureIsPropagatedToTheCaller() {
        RecordingWal localWal = new RecordingWal();
        StubReplicator replicator = new StubReplicator(null);
        ReplicatedWalWriter writer = new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TIMEOUT_NANOS);

        IOException error = assertThrows(IOException.class, () -> writer.append(command(1L)));

        assertEquals("replicator down", error.getMessage());
    }

    @Test
    void forceAndCloseOnlyTouchTheLocalWal() throws IOException {
        RecordingWal localWal = new RecordingWal();
        StubReplicator replicator = new StubReplicator(new ReplicationResult(1, 1, List.of("b"), List.of()));
        ReplicatedWalWriter writer = new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TIMEOUT_NANOS);

        writer.force();
        writer.close();

        assertEquals(List.of("force", "close"), localWal.events);
        assertTrue(replicator.sequences.isEmpty());
    }

    @Test
    void appendAllInheritedFromWalWriterReplicatesEveryCommand() throws IOException {
        RecordingWal localWal = new RecordingWal();
        StubReplicator replicator = new StubReplicator(new ReplicationResult(1, 1, List.of("b"), List.of()));
        ReplicatedWalWriter writer = new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TIMEOUT_NANOS);

        writer.appendAll(List.of(command(1L), command(2L)));

        assertEquals(List.of("append:1", "append:2"), localWal.events);
        assertEquals(List.of(1L, 2L), replicator.sequences);
    }

    @Test
    void constructorRejectsMissingCollaboratorsAndNegativeTimeouts() {
        RecordingWal localWal = new RecordingWal();
        StubReplicator replicator = new StubReplicator(new ReplicationResult(0, 0, List.of(), List.of()));

        assertThrows(NullPointerException.class, () -> new ReplicatedWalWriter(
                null, replicator, ReplicationMode.LOCAL_ONLY, 0L));
        assertThrows(NullPointerException.class, () -> new ReplicatedWalWriter(
                localWal, null, ReplicationMode.LOCAL_ONLY, 0L));
        assertThrows(NullPointerException.class, () -> new ReplicatedWalWriter(
                localWal, replicator, null, 0L));
        assertThrows(IllegalArgumentException.class, () -> new ReplicatedWalWriter(
                localWal, replicator, ReplicationMode.LOCAL_ONLY, -1L));
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 1_000L + sequence, 2_000L + sequence, 1,
                Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
    }

    private static final class RecordingWal implements WalWriter {
        private final List<String> events = new ArrayList<>();
        private boolean failOnAppend;

        @Override
        public void append(Command command) throws IOException {
            if (failOnAppend) {
                throw new IOException("local wal down");
            }
            events.add("append:" + command.sequence);
        }

        @Override
        public void force() {
            events.add("force");
        }

        @Override
        public void close() {
            events.add("close");
        }
    }

    /** Returns a fixed result, or fails when no result was configured. */
    private static final class StubReplicator implements CommandReplicator {
        private final List<Long> sequences = new ArrayList<>();
        private final List<Long> timeouts = new ArrayList<>();
        private final ReplicationResult result;

        private StubReplicator(ReplicationResult result) {
            this.result = result;
        }

        @Override
        public ReplicationResult replicate(Command command, long timeoutNanos) throws IOException {
            if (result == null) {
                throw new IOException("replicator down");
            }
            sequences.add(command.sequence);
            timeouts.add(timeoutNanos);
            return result;
        }
    }
}
