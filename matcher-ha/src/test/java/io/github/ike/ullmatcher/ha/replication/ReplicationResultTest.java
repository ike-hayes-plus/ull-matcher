package io.github.ike.ullmatcher.ha.replication;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ReplicationResultTest {
    @Test
    void localOnlyIsSatisfiedEvenWhenEveryStandbyFailed() {
        ReplicationResult allFailed = new ReplicationResult(3, 0, List.of(), List.of("b", "c", "d"));

        assertTrue(allFailed.satisfies(ReplicationMode.LOCAL_ONLY));
        assertFalse(allFailed.satisfies(ReplicationMode.WAIT_FOR_ANY_STANDBY));
        assertFalse(allFailed.satisfies(ReplicationMode.WAIT_FOR_QUORUM_STANDBYS));
        assertFalse(allFailed.satisfies(ReplicationMode.WAIT_FOR_ALL_STANDBYS));
    }

    @Test
    void oneAckOutOfThreeOnlySatisfiesTheWeakerModes() {
        ReplicationResult oneAck = new ReplicationResult(3, 1, List.of("b"), List.of("c", "d"));

        assertTrue(oneAck.satisfies(ReplicationMode.LOCAL_ONLY));
        assertTrue(oneAck.satisfies(ReplicationMode.WAIT_FOR_ANY_STANDBY));
        assertFalse(oneAck.satisfies(ReplicationMode.WAIT_FOR_QUORUM_STANDBYS));
        assertFalse(oneAck.satisfies(ReplicationMode.WAIT_FOR_ALL_STANDBYS));
    }

    @Test
    void quorumIsSatisfiedAtTwoOfThreeButAllStandbysIsNot() {
        ReplicationResult twoAcks = new ReplicationResult(3, 2, List.of("b", "c"), List.of("d"));

        assertTrue(twoAcks.satisfies(ReplicationMode.WAIT_FOR_QUORUM_STANDBYS));
        assertFalse(twoAcks.satisfies(ReplicationMode.WAIT_FOR_ALL_STANDBYS));
    }

    @Test
    void everyModeIsSatisfiedWithZeroStandbysConfigured() {
        ReplicationResult noTargets = new ReplicationResult(0, 0, List.of(), List.of());

        for (ReplicationMode mode : ReplicationMode.values()) {
            assertTrue(noTargets.satisfies(mode), mode + " must be satisfiable without standbys");
        }
    }

    @Test
    void nodeIdListsAreDefensivelyCopied() {
        List<String> acked = new ArrayList<>(List.of("b"));
        List<String> failed = new ArrayList<>(List.of("c"));
        ReplicationResult result = new ReplicationResult(2, 1, acked, failed);

        acked.add("mutated");
        failed.clear();

        assertEquals(List.of("b"), result.ackedNodeIds());
        assertEquals(List.of("c"), result.failedNodeIds());
        assertThrows(UnsupportedOperationException.class, () -> result.ackedNodeIds().add("x"));
    }

    @Test
    void resultsWithTheSameAcksAreEqual() {
        ReplicationResult first = new ReplicationResult(2, 1, List.of("b"), List.of("c"));
        ReplicationResult second = new ReplicationResult(2, 1, List.of("b"), List.of("c"));

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertTrue(first.toString().contains("ackedTargets=1"));
    }
}
