package io.github.ike.ullmatcher.ha.grpc.codec;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandType;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.grpc.proto.CommandEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.NodeControlStateEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationCursorEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.SnapshotChunkEnvelope;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import com.google.protobuf.ByteString;

public final class ProtoAdapters {
    private ProtoAdapters() {}

    public static CommandEnvelope toProto(Command command) {
        return CommandEnvelope.newBuilder()
                .setType(command.type.ordinal())
                .setSequence(command.sequence)
                .setOrderId(command.orderId)
                .setUserId(command.userId)
                .setSymbolId(command.symbolId)
                .setSide(command.side)
                .setOrderType(command.orderType)
                .setTimeInForce(command.timeInForce)
                .setPrice(command.price)
                .setQuantity(command.quantity)
                .build();
    }

    public static Command fromProto(CommandEnvelope envelope) {
        CommandType type = CommandType.values()[envelope.getType()];
        return switch (type) {
            case NEW_ORDER -> newOrder(envelope);
            case CANCEL_ORDER -> Command.cancel(envelope.getSequence(), envelope.getOrderId(), envelope.getSymbolId());
            case SNAPSHOT_MARKER -> Command.snapshotMarker(envelope.getSequence(), envelope.getSymbolId());
            case SHUTDOWN -> Command.shutdown(envelope.getSequence());
        };
    }

    public static ReplicationCursorEnvelope toProto(ReplicationCursor cursor) {
        return ReplicationCursorEnvelope.newBuilder()
                .setLastReceivedSequence(cursor.lastReceivedSequence())
                .setLastDurableSequence(cursor.lastDurableSequence())
                .setLastAppliedSequence(cursor.lastAppliedSequence())
                .setSnapshotSequence(cursor.snapshotSequence())
                .build();
    }

    public static ReplicationCursor fromProto(ReplicationCursorEnvelope envelope) {
        return new ReplicationCursor(
                envelope.getLastReceivedSequence(),
                envelope.getLastDurableSequence(),
                envelope.getLastAppliedSequence(),
                envelope.getSnapshotSequence()
        );
    }

    public static NodeControlStateEnvelope toProto(NodeControlState state) {
        return NodeControlStateEnvelope.newBuilder()
                .setNodeId(state.nodeId())
                .setRole(state.role().name())
                .setFencingEpoch(state.fencingToken().epoch())
                .setAcceptingClientCommands(state.acceptingClientCommands())
                .setLoopState(state.loopState().name())
                .setProcessedCommandCount(state.processedCommandCount())
                .setCursor(toProto(state.cursor()))
                .build();
    }

    public static NodeControlState fromProto(NodeControlStateEnvelope envelope) {
        return new NodeControlState(
                envelope.getNodeId(),
                HaRole.valueOf(envelope.getRole()),
                new FencingToken(Math.max(1L, envelope.getFencingEpoch())),
                envelope.getAcceptingClientCommands(),
                MatchLoopState.valueOf(envelope.getLoopState()),
                envelope.getProcessedCommandCount(),
                fromProto(envelope.getCursor())
        );
    }

    public static SnapshotChunkEnvelope snapshotChunk(long lastSequence,
                                              long lastTradeId,
                                              long liveOrderCount,
                                              long totalBytes,
                                              int chunkIndex,
                                              boolean lastChunk,
                                              byte[] payload) {
        return SnapshotChunkEnvelope.newBuilder()
                .setLastSequence(lastSequence)
                .setLastTradeId(lastTradeId)
                .setLiveOrderCount(liveOrderCount)
                .setTotalBytes(totalBytes)
                .setChunkIndex(chunkIndex)
                .setLastChunk(lastChunk)
                .setPayload(ByteString.copyFrom(payload))
                .build();
    }

    private static Command newOrder(CommandEnvelope envelope) {
        return Command.newOrder(
                envelope.getSequence(),
                envelope.getOrderId(),
                envelope.getUserId(),
                envelope.getSymbolId(),
                side(envelope.getSide()),
                orderType(envelope.getOrderType()),
                timeInForce(envelope.getTimeInForce()),
                envelope.getPrice(),
                envelope.getQuantity()
        );
    }

    private static io.github.ike.ullmatcher.api.Side side(int code) {
        for (io.github.ike.ullmatcher.api.Side value : io.github.ike.ullmatcher.api.Side.values()) {
            if (value.code == (byte) code) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown side code " + code);
    }

    private static io.github.ike.ullmatcher.api.OrderType orderType(int code) {
        for (io.github.ike.ullmatcher.api.OrderType value : io.github.ike.ullmatcher.api.OrderType.values()) {
            if (value.code == (byte) code) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown orderType code " + code);
    }

    private static io.github.ike.ullmatcher.api.TimeInForce timeInForce(int code) {
        for (io.github.ike.ullmatcher.api.TimeInForce value : io.github.ike.ullmatcher.api.TimeInForce.values()) {
            if (value.code == (byte) code) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown timeInForce code " + code);
    }
}
