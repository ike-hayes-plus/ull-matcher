package io.github.ike.ullmatcher.ha.state;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ReplicaStateTest {
    private static final ReplicationCursor CURSOR = new ReplicationCursor(5L, 5L, 5L, 5L);

    @Test
    void aHeartbeatAtTimeZeroIsValid() {
        ReplicaState state = new ReplicaState(
                "node-a", HaRole.PRIMARY, true, true, 0L, new FencingToken(1L), CURSOR);

        assertEquals(0L, state.lastHeartbeatNanos());
        assertEquals(5L, state.cursor().promotionWatermark());
    }

    @Test
    void negativeHeartbeatTimestampsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicaState(
                "node-a", HaRole.PRIMARY, true, true, -1L, new FencingToken(1L), CURSOR));
    }

    @Test
    void everyRequiredFieldIsChecked() {
        FencingToken token = new FencingToken(1L);

        assertThrows(NullPointerException.class, () -> new ReplicaState(
                null, HaRole.PRIMARY, true, true, 0L, token, CURSOR));
        assertThrows(NullPointerException.class, () -> new ReplicaState(
                "node-a", null, true, true, 0L, token, CURSOR));
        assertThrows(NullPointerException.class, () -> new ReplicaState(
                "node-a", HaRole.PRIMARY, true, true, 0L, null, CURSOR));
        assertThrows(NullPointerException.class, () -> new ReplicaState(
                "node-a", HaRole.PRIMARY, true, true, 0L, token, null));
    }
}
