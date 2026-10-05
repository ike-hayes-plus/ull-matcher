package io.github.ike.ullmatcher.ha.grpc.codec;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandType;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.proto.CommandEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.NodeControlStateEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.ReplicationCursorEnvelope;
import io.github.ike.ullmatcher.ha.grpc.proto.SnapshotChunkEnvelope;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProtoAdaptersTest {
    @Test
    void newOrderSurvivesAWireRoundTrip() {
        Command command = Command.newOrder(
                11L, 1_001L, 2_002L, 7, Side.SELL, OrderType.MARKET_WITH_PROTECTION, TimeInForce.POST_ONLY, 980L, 42L);

        Command decoded = ProtoAdapters.fromProto(ProtoAdapters.toProto(command));

        assertEquals(CommandType.NEW_ORDER, decoded.type);
        assertEquals(11L, decoded.sequence);
        assertEquals(1_001L, decoded.orderId);
        assertEquals(2_002L, decoded.userId);
        assertEquals(7, decoded.symbolId);
        assertEquals(Side.SELL.code, decoded.side);
        assertEquals(OrderType.MARKET_WITH_PROTECTION.code, decoded.orderType);
        assertEquals(TimeInForce.POST_ONLY.code, decoded.timeInForce);
        assertEquals(980L, decoded.price);
        assertEquals(42L, decoded.quantity);
    }

    @Test
    void everyTimeInForceAndSideCodeIsDecodable() {
        for (Side side : Side.values()) {
            for (OrderType orderType : OrderType.values()) {
                for (TimeInForce timeInForce : TimeInForce.values()) {
                    Command command = Command.newOrder(1L, 2L, 3L, 4, side, orderType, timeInForce, 5L, 6L);

                    Command decoded = ProtoAdapters.fromProto(ProtoAdapters.toProto(command));

                    assertEquals(side.code, decoded.side);
                    assertEquals(orderType.code, decoded.orderType);
                    assertEquals(timeInForce.code, decoded.timeInForce);
                }
            }
        }
    }

    @Test
    void controlCommandsKeepTheirTypeAndSequence() {
        Command cancel = ProtoAdapters.fromProto(ProtoAdapters.toProto(Command.cancel(5L, 1_001L, 7)));
        Command marker = ProtoAdapters.fromProto(ProtoAdapters.toProto(Command.snapshotMarker(6L, 7)));
        Command shutdown = ProtoAdapters.fromProto(ProtoAdapters.toProto(Command.shutdown(7L)));

        assertEquals(CommandType.CANCEL_ORDER, cancel.type);
        assertEquals(1_001L, cancel.orderId);
        assertEquals(7, cancel.symbolId);
        assertEquals(CommandType.SNAPSHOT_MARKER, marker.type);
        assertEquals(6L, marker.sequence);
        assertEquals(CommandType.SHUTDOWN, shutdown.type);
        assertEquals(7L, shutdown.sequence);
    }

    @Test
    void unknownWireCodesAreRejectedInsteadOfSilentlyDefaulted() {
        CommandEnvelope.Builder template = CommandEnvelope.newBuilder()
                .setType(CommandType.NEW_ORDER.ordinal())
                .setSequence(1L)
                .setOrderId(2L)
                .setUserId(3L)
                .setSymbolId(4)
                .setSide(Side.BUY.code)
                .setOrderType(OrderType.LIMIT.code)
                .setTimeInForce(TimeInForce.GTC.code)
                .setPrice(5L)
                .setQuantity(6L);

        assertEquals("unknown side code 99", assertThrows(IllegalArgumentException.class,
                () -> ProtoAdapters.fromProto(template.clone().setSide(99).build())).getMessage());
        assertEquals("unknown orderType code 99", assertThrows(IllegalArgumentException.class,
                () -> ProtoAdapters.fromProto(template.clone().setOrderType(99).build())).getMessage());
        assertEquals("unknown timeInForce code 99", assertThrows(IllegalArgumentException.class,
                () -> ProtoAdapters.fromProto(template.clone().setTimeInForce(99).build())).getMessage());
    }

    @Test
    void unknownCommandTypeOrdinalIsRejected() {
        CommandEnvelope envelope = CommandEnvelope.newBuilder().setType(42).setSequence(1L).build();

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> ProtoAdapters.fromProto(envelope));
    }

    @Test
    void replicationCursorSurvivesAWireRoundTrip() {
        ReplicationCursor cursor = new ReplicationCursor(90L, 80L, 70L, 60L);

        ReplicationCursorEnvelope envelope = ProtoAdapters.toProto(cursor);

        assertEquals(90L, envelope.getLastReceivedSequence());
        assertEquals(80L, envelope.getLastDurableSequence());
        assertEquals(70L, envelope.getLastAppliedSequence());
        assertEquals(60L, envelope.getSnapshotSequence());
        assertEquals(cursor, ProtoAdapters.fromProto(envelope));
    }

    @Test
    void nodeControlStateSurvivesAWireRoundTrip() {
        NodeControlState state = new NodeControlState(
                "standby-a",
                HaRole.CATCHING_UP,
                new FencingToken(9L),
                true,
                MatchLoopState.RUNNING,
                123L,
                new ReplicationCursor(9L, 8L, 7L, 6L)
        );

        NodeControlStateEnvelope envelope = ProtoAdapters.toProto(state);

        assertEquals("CATCHING_UP", envelope.getRole());
        assertEquals("RUNNING", envelope.getLoopState());
        assertEquals(state, ProtoAdapters.fromProto(envelope));
    }

    @Test
    void missingFencingEpochIsClampedToTheFirstEpoch() {
        NodeControlStateEnvelope envelope = NodeControlStateEnvelope.newBuilder()
                .setNodeId("standby-a")
                .setRole(HaRole.STANDBY.name())
                .setFencingEpoch(0L)
                .setAcceptingClientCommands(false)
                .setLoopState(MatchLoopState.STOPPED.name())
                .setProcessedCommandCount(0L)
                .setCursor(ProtoAdapters.toProto(new ReplicationCursor(0L, 0L, 0L, 0L)))
                .build();

        NodeControlState state = ProtoAdapters.fromProto(envelope);

        assertEquals(new FencingToken(1L), state.fencingToken(),
                "a default-valued epoch must not produce an invalid fencing token");
    }

    @Test
    void snapshotChunkCarriesMetadataAndPayload() {
        byte[] payload = "snapshot-bytes".getBytes(StandardCharsets.UTF_8);

        SnapshotChunkEnvelope chunk = ProtoAdapters.snapshotChunk(42L, 7L, 3L, 4_096L, 2, true, payload);

        assertEquals(42L, chunk.getLastSequence());
        assertEquals(7L, chunk.getLastTradeId());
        assertEquals(3L, chunk.getLiveOrderCount());
        assertEquals(4_096L, chunk.getTotalBytes());
        assertEquals(2, chunk.getChunkIndex());
        assertTrue(chunk.getLastChunk());
        assertArrayEquals(payload, chunk.getPayload().toByteArray());
    }
}
