package io.github.ike.ullmatcher.ha.aeron;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 编解码节点控制面返回的运行状态快照。
 */
public final class AeronNodeControlStateCodec {
    private static final int OFFSET_REQUEST_ID = 0;
    private static final int OFFSET_ROLE = OFFSET_REQUEST_ID + Long.BYTES;
    private static final int OFFSET_FENCING_EPOCH = OFFSET_ROLE + Integer.BYTES;
    private static final int OFFSET_ACCEPTING_CLIENT_COMMANDS = OFFSET_FENCING_EPOCH + Long.BYTES;
    private static final int OFFSET_LOOP_STATE = OFFSET_ACCEPTING_CLIENT_COMMANDS + Integer.BYTES;
    private static final int OFFSET_PROCESSED_COMMAND_COUNT = OFFSET_LOOP_STATE + Integer.BYTES;
    private static final int OFFSET_LAST_RECEIVED_SEQUENCE = OFFSET_PROCESSED_COMMAND_COUNT + Long.BYTES;
    private static final int OFFSET_LAST_DURABLE_SEQUENCE = OFFSET_LAST_RECEIVED_SEQUENCE + Long.BYTES;
    private static final int OFFSET_LAST_APPLIED_SEQUENCE = OFFSET_LAST_DURABLE_SEQUENCE + Long.BYTES;
    private static final int OFFSET_SNAPSHOT_SEQUENCE = OFFSET_LAST_APPLIED_SEQUENCE + Long.BYTES;
    private static final int OFFSET_NODE_ID_LENGTH = OFFSET_SNAPSHOT_SEQUENCE + Long.BYTES;
    private static final int OFFSET_NODE_ID = OFFSET_NODE_ID_LENGTH + Integer.BYTES;

    private AeronNodeControlStateCodec() {
    }

    public static UnsafeBuffer allocateBuffer(int nodeIdLength) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength(nodeIdLength)));
    }

    public static int encodedLength(int nodeIdLength) {
        return OFFSET_NODE_ID + nodeIdLength;
    }

    public static int encode(long requestId, NodeControlState state, MutableDirectBuffer buffer) {
        byte[] nodeIdBytes = state.nodeId().getBytes(StandardCharsets.UTF_8);
        if (buffer.capacity() < encodedLength(nodeIdBytes.length)) {
            throw new IllegalArgumentException("buffer capacity too small for node control state");
        }
        buffer.putLong(OFFSET_REQUEST_ID, requestId);
        buffer.putInt(OFFSET_ROLE, state.role().ordinal());
        buffer.putLong(OFFSET_FENCING_EPOCH, state.fencingToken().epoch());
        buffer.putInt(OFFSET_ACCEPTING_CLIENT_COMMANDS, state.acceptingClientCommands() ? 1 : 0);
        buffer.putInt(OFFSET_LOOP_STATE, state.loopState().ordinal());
        buffer.putLong(OFFSET_PROCESSED_COMMAND_COUNT, state.processedCommandCount());
        buffer.putLong(OFFSET_LAST_RECEIVED_SEQUENCE, state.cursor().lastReceivedSequence());
        buffer.putLong(OFFSET_LAST_DURABLE_SEQUENCE, state.cursor().lastDurableSequence());
        buffer.putLong(OFFSET_LAST_APPLIED_SEQUENCE, state.cursor().lastAppliedSequence());
        buffer.putLong(OFFSET_SNAPSHOT_SEQUENCE, state.cursor().snapshotSequence());
        buffer.putInt(OFFSET_NODE_ID_LENGTH, nodeIdBytes.length);
        buffer.putBytes(OFFSET_NODE_ID, nodeIdBytes);
        return encodedLength(nodeIdBytes.length);
    }

    public static DecodedState decode(DirectBuffer buffer, int offset, int length) {
        long requestId = buffer.getLong(offset + OFFSET_REQUEST_ID);
        HaRole role = HaRole.values()[buffer.getInt(offset + OFFSET_ROLE)];
        long fencingEpoch = buffer.getLong(offset + OFFSET_FENCING_EPOCH);
        boolean acceptingClientCommands = buffer.getInt(offset + OFFSET_ACCEPTING_CLIENT_COMMANDS) == 1;
        MatchLoopState loopState = MatchLoopState.values()[buffer.getInt(offset + OFFSET_LOOP_STATE)];
        long processedCommandCount = buffer.getLong(offset + OFFSET_PROCESSED_COMMAND_COUNT);
        long lastReceivedSequence = buffer.getLong(offset + OFFSET_LAST_RECEIVED_SEQUENCE);
        long lastDurableSequence = buffer.getLong(offset + OFFSET_LAST_DURABLE_SEQUENCE);
        long lastAppliedSequence = buffer.getLong(offset + OFFSET_LAST_APPLIED_SEQUENCE);
        long snapshotSequence = buffer.getLong(offset + OFFSET_SNAPSHOT_SEQUENCE);
        int nodeIdLength = buffer.getInt(offset + OFFSET_NODE_ID_LENGTH);
        byte[] nodeIdBytes = new byte[nodeIdLength];
        buffer.getBytes(offset + OFFSET_NODE_ID, nodeIdBytes);
        NodeControlState state = new NodeControlState(
                new String(nodeIdBytes, StandardCharsets.UTF_8),
                role,
                new FencingToken(Math.max(1L, fencingEpoch)),
                acceptingClientCommands,
                loopState,
                processedCommandCount,
                new ReplicationCursor(lastReceivedSequence, lastDurableSequence, lastAppliedSequence, snapshotSequence)
        );
        return new DecodedState(requestId, state);
    }

    public record DecodedState(long requestId, NodeControlState state) {
    }
}
