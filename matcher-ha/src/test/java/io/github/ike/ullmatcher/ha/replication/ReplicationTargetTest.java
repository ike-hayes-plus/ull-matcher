package io.github.ike.ullmatcher.ha.replication;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ReplicationTargetTest {
    @Test
    void defaultBatchForwardsCommandsInOrderWithTheSameBudget() throws IOException {
        RecordingTarget target = new RecordingTarget();

        target.replicateBatch(List.of(command(1L), command(2L), command(3L)), 7L);

        assertEquals("standby-a", target.nodeId());
        assertEquals(List.of(1L, 2L, 3L), target.sequences);
        assertEquals(List.of(7L, 7L, 7L), target.timeouts);
    }

    @Test
    void defaultBatchAcceptsNullAndEmptyWithoutReplicating() throws IOException {
        RecordingTarget target = new RecordingTarget();

        target.replicateBatch(null, 1L);
        target.replicateBatch(List.of(), 1L);

        assertTrue(target.sequences.isEmpty());
    }

    @Test
    void defaultBatchStopsAtTheFirstFailureSoTheCallerCanRetryFromTheGap() {
        RecordingTarget target = new RecordingTarget();
        target.failOnSequence = 2L;

        IOException error = assertThrows(IOException.class,
                () -> target.replicateBatch(List.of(command(1L), command(2L), command(3L)), 1L));

        assertEquals("standby-a rejected 2", error.getMessage());
        assertEquals(List.of(1L), target.sequences);
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 1_000L + sequence, 2_000L + sequence, 1,
                Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
    }

    private static final class RecordingTarget implements ReplicationTarget {
        private final List<Long> sequences = new ArrayList<>();
        private final List<Long> timeouts = new ArrayList<>();
        private long failOnSequence = -1L;

        @Override
        public String nodeId() {
            return "standby-a";
        }

        @Override
        public void replicate(Command command, long timeoutNanos) throws IOException {
            if (command.sequence == failOnSequence) {
                throw new IOException(nodeId() + " rejected " + command.sequence);
            }
            sequences.add(command.sequence);
            timeouts.add(timeoutNanos);
        }
    }
}
