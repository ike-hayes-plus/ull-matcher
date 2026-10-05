package io.github.ike.ullmatcher.ha.aeron;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandType;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the single-command replication frame. The frame kind discriminates this frame
 * from the batch frame on the same stream, so a mismatched kind must be rejected.
 */
final class AeronReplicatedCommandCodecTest {
    @Test
    void roundTripsCommandAndReturnAddress() {
        Command command = Command.newOrder(
                42L, 1001L, 2002L, 7, Side.SELL, OrderType.MARKET_WITH_PROTECTION,
                TimeInForce.IOC, 123_456L, 789L, 1_700_000_000_000L);
        String responseChannel = "aeron:udp?endpoint=127.0.0.1:20401";

        UnsafeBuffer buffer = AeronReplicatedCommandCodec.allocateBuffer(
                responseChannel.getBytes(StandardCharsets.UTF_8).length);
        int encodedLength = AeronReplicatedCommandCodec.encode(command, responseChannel, 555, buffer);

        assertEquals(buffer.capacity(), encodedLength);
        assertEquals(AeronReplicatedCommandCodec.FRAME_KIND_SINGLE,
                AeronReplicatedCommandCodec.frameKind(buffer, 0));

        AeronReplicatedCommandCodec.DecodedCommand decoded =
                AeronReplicatedCommandCodec.decode(buffer, 0, encodedLength);

        assertEquals(responseChannel, decoded.responseChannel());
        assertEquals(555, decoded.responseStreamId());
        assertCommandEquals(command, decoded.command());
    }

    @Test
    void encodedLengthIsTheHeaderPlusChannelPlusFixedCommand() {
        assertEquals(Integer.BYTES * 3 + AeronCommandCodec.ENCODED_LENGTH,
                AeronReplicatedCommandCodec.encodedLength(0));
        assertEquals(AeronReplicatedCommandCodec.encodedLength(0) + 9,
                AeronReplicatedCommandCodec.encodedLength(9));
    }

    @Test
    void roundTripsEmptyResponseChannel() {
        Command command = Command.shutdown(Long.MAX_VALUE);

        UnsafeBuffer buffer = AeronReplicatedCommandCodec.allocateBuffer(0);
        int encodedLength = AeronReplicatedCommandCodec.encode(command, "", Integer.MIN_VALUE, buffer);

        AeronReplicatedCommandCodec.DecodedCommand decoded =
                AeronReplicatedCommandCodec.decode(buffer, 0, encodedLength);

        assertEquals("", decoded.responseChannel());
        assertEquals(Integer.MIN_VALUE, decoded.responseStreamId());
        assertEquals(CommandType.SHUTDOWN, decoded.command().type);
        assertEquals(Long.MAX_VALUE, decoded.command().sequence);
    }

    @Test
    void roundTripsMultiByteResponseChannel() {
        String responseChannel = "aeron:ipc?别名=节点";
        int channelLength = responseChannel.getBytes(StandardCharsets.UTF_8).length;
        Command command = Command.cancel(5L, 6L, 7);

        UnsafeBuffer buffer = AeronReplicatedCommandCodec.allocateBuffer(channelLength);
        int encodedLength = AeronReplicatedCommandCodec.encode(command, responseChannel, 1, buffer);

        assertEquals(responseChannel,
                AeronReplicatedCommandCodec.decode(buffer, 0, encodedLength).responseChannel());
    }

    @Test
    void decodesFromNonZeroOffset() {
        Command command = Command.snapshotMarker(99L, 3);
        String responseChannel = "aeron:ipc";
        UnsafeBuffer encoded = AeronReplicatedCommandCodec.allocateBuffer(responseChannel.length());
        int encodedLength = AeronReplicatedCommandCodec.encode(command, responseChannel, 17, encoded);

        int offset = 20;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        assertEquals(AeronReplicatedCommandCodec.FRAME_KIND_SINGLE,
                AeronReplicatedCommandCodec.frameKind(framed, offset));

        AeronReplicatedCommandCodec.DecodedCommand decoded =
                AeronReplicatedCommandCodec.decode(framed, offset, encodedLength);

        assertEquals(responseChannel, decoded.responseChannel());
        assertEquals(17, decoded.responseStreamId());
        assertCommandEquals(command, decoded.command());
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        Command command = Command.shutdown(1L);
        String responseChannel = "aeron:ipc";
        UnsafeBuffer tooSmall = new UnsafeBuffer(new byte[AeronReplicatedCommandCodec.encodedLength(
                responseChannel.getBytes(StandardCharsets.UTF_8).length) - 1]);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandCodec.encode(command, responseChannel, 1, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsForeignFrameKind() {
        UnsafeBuffer buffer = AeronReplicatedCommandCodec.allocateBuffer(0);
        int encodedLength = AeronReplicatedCommandCodec.encode(Command.shutdown(1L), "", 0, buffer);
        buffer.putInt(0, AeronReplicatedCommandBatchCodec.FRAME_KIND_BATCH);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandCodec.decode(buffer, 0, encodedLength));

        assertTrue(failure.getMessage().contains("unsupported replicated command frame kind"));
    }

    @Test
    void rejectsTruncatedFrame() {
        String responseChannel = "aeron:ipc";
        UnsafeBuffer encoded = AeronReplicatedCommandCodec.allocateBuffer(responseChannel.length());
        int encodedLength = AeronReplicatedCommandCodec.encode(
                Command.cancel(1L, 2L, 3), responseChannel, 1, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 8]);
        truncated.putBytes(0, encoded, 0, encodedLength - 8);

        assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandCodec.decode(truncated, 0, encodedLength - 8));
    }

    @Test
    void rejectsChannelLengthThatWouldAllocatePastTheFrame() {
        UnsafeBuffer buffer = AeronReplicatedCommandCodec.allocateBuffer(1);
        int encodedLength = AeronReplicatedCommandCodec.encode(Command.shutdown(1L), "a", 1, buffer);
        buffer.putInt(8, Integer.MAX_VALUE);

        assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandCodec.decode(buffer, 0, encodedLength));
    }

    static void assertCommandEquals(Command expected, Command actual) {
        assertEquals(expected.type, actual.type);
        assertEquals(expected.sequence, actual.sequence);
        assertEquals(expected.orderId, actual.orderId);
        assertEquals(expected.userId, actual.userId);
        assertEquals(expected.symbolId, actual.symbolId);
        assertEquals(expected.side, actual.side);
        assertEquals(expected.orderType, actual.orderType);
        assertEquals(expected.timeInForce, actual.timeInForce);
        assertEquals(expected.price, actual.price);
        assertEquals(expected.quantity, actual.quantity);
        assertEquals(expected.expireAtEpochMillis, actual.expireAtEpochMillis);
    }
}
