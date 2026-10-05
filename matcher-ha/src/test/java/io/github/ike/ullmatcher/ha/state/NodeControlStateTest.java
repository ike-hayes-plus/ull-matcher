package io.github.ike.ullmatcher.ha.state;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Control-plane snapshots are what operators and peers read to decide whether a node is safe to
 * promote, so a snapshot must never be constructible in a self-contradictory or partial shape.
 */
final class NodeControlStateTest {
    private static final ReplicationCursor CURSOR = new ReplicationCursor(10L, 9L, 8L, 5L);

    @Test
    void snapshotKeepsEveryFieldItWasBuiltWith() {
        NodeControlState state = new NodeControlState(
                "node-a", HaRole.PRIMARY, new FencingToken(7L), true, MatchLoopState.RUNNING, 42L, CURSOR);

        assertEquals("node-a", state.nodeId());
        assertEquals(HaRole.PRIMARY, state.role());
        assertEquals(7L, state.fencingToken().epoch());
        assertTrue(state.acceptingClientCommands());
        assertEquals(MatchLoopState.RUNNING, state.loopState());
        assertEquals(42L, state.processedCommandCount());
        assertEquals(CURSOR, state.cursor());
    }

    @Test
    void aFencedNodeIsRepresentableAsNotAcceptingCommands() {
        NodeControlState fenced = new NodeControlState(
                "node-a", HaRole.FENCED, new FencingToken(1L), false, MatchLoopState.STOPPED, 0L, CURSOR);

        assertEquals(HaRole.FENCED, fenced.role());
        assertFalse(fenced.acceptingClientCommands());
        assertEquals(MatchLoopState.STOPPED, fenced.loopState());
    }

    @Test
    void zeroProcessedCommandsIsValidButNegativeIsNot() {
        assertEquals(0L, new NodeControlState(
                "node-a", HaRole.STANDBY, new FencingToken(1L), false,
                MatchLoopState.QUIESCING, 0L, CURSOR).processedCommandCount());

        assertThrows(IllegalArgumentException.class, () -> new NodeControlState(
                "node-a", HaRole.STANDBY, new FencingToken(1L), false,
                MatchLoopState.QUIESCING, -1L, CURSOR));
    }

    @Test
    void everyRequiredFieldIsChecked() {
        FencingToken token = new FencingToken(1L);

        assertThrows(NullPointerException.class, () -> new NodeControlState(
                null, HaRole.STANDBY, token, false, MatchLoopState.RUNNING, 0L, CURSOR));
        assertThrows(NullPointerException.class, () -> new NodeControlState(
                "node-a", null, token, false, MatchLoopState.RUNNING, 0L, CURSOR));
        assertThrows(NullPointerException.class, () -> new NodeControlState(
                "node-a", HaRole.STANDBY, null, false, MatchLoopState.RUNNING, 0L, CURSOR));
        assertThrows(NullPointerException.class, () -> new NodeControlState(
                "node-a", HaRole.STANDBY, token, false, null, 0L, CURSOR));
        assertThrows(NullPointerException.class, () -> new NodeControlState(
                "node-a", HaRole.STANDBY, token, false, MatchLoopState.RUNNING, 0L, null));
    }

    @Test
    void snapshotsOfTheSameNodeStateAreEqual() {
        NodeControlState first = new NodeControlState(
                "node-a", HaRole.STANDBY, new FencingToken(3L), false, MatchLoopState.QUIESCING, 5L, CURSOR);
        NodeControlState second = new NodeControlState(
                "node-a", HaRole.STANDBY, new FencingToken(3L), false, MatchLoopState.QUIESCING, 5L, CURSOR);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertTrue(first.toString().contains("node-a"));
    }

    @Test
    void aSourceCanPublishItsOwnSnapshot() {
        NodeControlState published = new NodeControlState(
                "node-b", HaRole.CATCHING_UP, new FencingToken(2L), false, MatchLoopState.QUIESCING, 1L, CURSOR);
        NodeControlStateSource source = () -> published;

        assertEquals(published, source.currentState());
    }
}
