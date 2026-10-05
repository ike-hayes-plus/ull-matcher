package io.github.ike.ullmatcher.ha.aeron;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the control-plane state snapshot frame. Role and loop state travel as ordinals,
 * so every declared enum constant must survive the round trip.
 */
final class AeronNodeControlStateCodecTest {
    @Test
    void roundTripsEveryField() {
        NodeControlState state = new NodeControlState(
                "node-primary",
                HaRole.PRIMARY,
                new FencingToken(17L),
                true,
                MatchLoopState.RUNNING,
                123_456_789L,
                new ReplicationCursor(900L, 800L, 700L, 600L)
        );

        UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(
                "node-primary".getBytes(StandardCharsets.UTF_8).length);
        int encodedLength = AeronNodeControlStateCodec.encode(4242L, state, buffer);

        assertEquals(buffer.capacity(), encodedLength);

        AeronNodeControlStateCodec.DecodedState decoded =
                AeronNodeControlStateCodec.decode(buffer, 0, encodedLength);

        assertEquals(4242L, decoded.requestId());
        assertEquals(state, decoded.state());
    }

    @Test
    void roundTripsEveryRoleAndLoopState() {
        for (HaRole role : HaRole.values()) {
            for (MatchLoopState loopState : MatchLoopState.values()) {
                NodeControlState state = new NodeControlState(
                        "n", role, new FencingToken(3L), false, loopState, 0L,
                        new ReplicationCursor(0L, 0L, 0L, 0L));

                UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(1);
                int encodedLength = AeronNodeControlStateCodec.encode(1L, state, buffer);

                AeronNodeControlStateCodec.DecodedState decoded =
                        AeronNodeControlStateCodec.decode(buffer, 0, encodedLength);

                assertEquals(role, decoded.state().role());
                assertEquals(loopState, decoded.state().loopState());
                assertFalse(decoded.state().acceptingClientCommands());
            }
        }
    }

    @Test
    void acceptingClientCommandsFlagRoundTripsBothWays() {
        assertTrue(roundTripAcceptingFlag(true));
        assertFalse(roundTripAcceptingFlag(false));
    }

    @Test
    void roundTripsMaximumSequenceValues() {
        NodeControlState state = new NodeControlState(
                "node-max",
                HaRole.FENCED,
                new FencingToken(Long.MAX_VALUE),
                true,
                MatchLoopState.FAILED,
                Long.MAX_VALUE,
                new ReplicationCursor(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)
        );

        UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(
                "node-max".getBytes(StandardCharsets.UTF_8).length);
        int encodedLength = AeronNodeControlStateCodec.encode(Long.MIN_VALUE, state, buffer);

        AeronNodeControlStateCodec.DecodedState decoded =
                AeronNodeControlStateCodec.decode(buffer, 0, encodedLength);

        assertEquals(Long.MIN_VALUE, decoded.requestId());
        assertEquals(state, decoded.state());
    }

    @Test
    void roundTripsMultiByteNodeId() {
        String nodeId = "节点-\uD83D\uDE80";
        int nodeIdLength = nodeId.getBytes(StandardCharsets.UTF_8).length;
        NodeControlState state = new NodeControlState(
                nodeId, HaRole.CATCHING_UP, new FencingToken(1L), true, MatchLoopState.STARTING, 5L,
                new ReplicationCursor(1L, 1L, 1L, 1L));

        UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(nodeIdLength);
        int encodedLength = AeronNodeControlStateCodec.encode(9L, state, buffer);

        assertEquals(AeronNodeControlStateCodec.encodedLength(nodeIdLength), encodedLength);
        assertEquals(nodeId, AeronNodeControlStateCodec.decode(buffer, 0, encodedLength).state().nodeId());
    }

    @Test
    void decodesFromNonZeroOffset() {
        NodeControlState state = new NodeControlState(
                "node-x", HaRole.STANDBY, new FencingToken(2L), true, MatchLoopState.DRAINING, 42L,
                new ReplicationCursor(5L, 4L, 3L, 2L));

        UnsafeBuffer encoded = AeronNodeControlStateCodec.allocateBuffer(6);
        int encodedLength = AeronNodeControlStateCodec.encode(8L, state, encoded);

        int offset = 32;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        AeronNodeControlStateCodec.DecodedState decoded =
                AeronNodeControlStateCodec.decode(framed, offset, encodedLength);

        assertEquals(8L, decoded.requestId());
        assertEquals(state, decoded.state());
    }

    @Test
    void rejectsNonPositiveFencingEpoch() {
        NodeControlState state = new NodeControlState(
                "node-y", HaRole.PRIMARY, new FencingToken(5L), true, MatchLoopState.RUNNING, 1L,
                new ReplicationCursor(0L, 0L, 0L, 0L));

        UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(6);
        int encodedLength = AeronNodeControlStateCodec.encode(1L, state, buffer);
        buffer.putLong(Long.BYTES + Integer.BYTES, 0L);

        IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
                () -> AeronNodeControlStateCodec.decode(buffer, 0, encodedLength));
        assertTrue(zero.getMessage().contains("fencing epoch must be positive"));

        buffer.putLong(Long.BYTES + Integer.BYTES, -1L);
        assertThrows(IllegalArgumentException.class,
                () -> AeronNodeControlStateCodec.decode(buffer, 0, encodedLength));
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        NodeControlState state = new NodeControlState(
                "node-long-identifier", HaRole.PRIMARY, new FencingToken(1L), true,
                MatchLoopState.RUNNING, 0L, new ReplicationCursor(0L, 0L, 0L, 0L));
        UnsafeBuffer tooSmall = new UnsafeBuffer(new byte[AeronNodeControlStateCodec.encodedLength(1)]);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronNodeControlStateCodec.encode(1L, state, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsTruncatedFrame() {
        NodeControlState state = new NodeControlState(
                "node-z", HaRole.PRIMARY, new FencingToken(1L), true, MatchLoopState.RUNNING, 0L,
                new ReplicationCursor(0L, 0L, 0L, 0L));
        UnsafeBuffer encoded = AeronNodeControlStateCodec.allocateBuffer(6);
        int encodedLength = AeronNodeControlStateCodec.encode(1L, state, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 3]);
        truncated.putBytes(0, encoded, 0, encodedLength - 3);

        assertThrows(IllegalArgumentException.class,
                () -> AeronNodeControlStateCodec.decode(truncated, 0, encodedLength - 3));
    }

    @Test
    void rejectsNodeIdLengthThatWouldAllocatePastTheFrame() {
        NodeControlState state = new NodeControlState(
                "node-z", HaRole.PRIMARY, new FencingToken(1L), true, MatchLoopState.RUNNING, 0L,
                new ReplicationCursor(0L, 0L, 0L, 0L));
        UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(6);
        int encodedLength = AeronNodeControlStateCodec.encode(1L, state, buffer);
        buffer.putInt(encodedLength - 6 - Integer.BYTES, Integer.MAX_VALUE);

        assertThrows(IllegalArgumentException.class,
                () -> AeronNodeControlStateCodec.decode(buffer, 0, encodedLength));
    }

    private static boolean roundTripAcceptingFlag(boolean accepting) {
        NodeControlState state = new NodeControlState(
                "node", HaRole.PRIMARY, new FencingToken(1L), accepting, MatchLoopState.RUNNING, 0L,
                new ReplicationCursor(0L, 0L, 0L, 0L));

        UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(4);
        int encodedLength = AeronNodeControlStateCodec.encode(1L, state, buffer);

        return AeronNodeControlStateCodec.decode(buffer, 0, encodedLength).state().acceptingClientCommands();
    }
}
