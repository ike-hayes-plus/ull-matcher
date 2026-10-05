package io.github.ike.ullmatcher.ha.aeron;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandType;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.github.ike.ullmatcher.ha.aeron.AeronReplicatedCommandCodecTest.assertCommandEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the batched replication frame. Decoding returns a flyweight list that reads
 * commands straight out of the buffer, so indexing and iteration must both stay inside the frame.
 */
final class AeronReplicatedCommandBatchCodecTest {
    @Test
    void roundTripsEveryCommandInOrder() {
        List<Command> commands = List.of(
                Command.newOrder(1L, 11L, 21L, 3, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 5L),
                Command.cancel(2L, 12L, 3),
                Command.snapshotMarker(3L, 3),
                Command.shutdown(4L));
        String responseChannel = "aeron:udp?endpoint=127.0.0.1:20501";
        int channelLength = responseChannel.getBytes(StandardCharsets.UTF_8).length;

        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(channelLength, commands.size());
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(commands, responseChannel, 909, buffer);

        assertEquals(AeronReplicatedCommandBatchCodec.encodedLength(channelLength, commands.size()), encodedLength);
        assertEquals(buffer.capacity(), encodedLength);

        AeronReplicatedCommandBatchCodec.DecodedBatch decoded =
                AeronReplicatedCommandBatchCodec.decode(buffer, 0, encodedLength);

        assertEquals(responseChannel, decoded.responseChannel());
        assertEquals(909, decoded.responseStreamId());
        assertEquals(commands.size(), decoded.commands().size());
        assertFalse(decoded.commands().isEmpty());
        for (int i = 0; i < commands.size(); i++) {
            assertCommandEquals(commands.get(i), decoded.commands().get(i));
        }
    }

    @Test
    void decodedCommandListSupportsIteration() {
        List<Command> commands = List.of(
                Command.newOrder(10L, 1L, 2L, 1, Side.SELL, OrderType.MARKET_WITH_PROTECTION,
                        TimeInForce.FOK, 1L, 1L, 99L),
                Command.newOrder(11L, 2L, 3L, 1, Side.BUY, OrderType.LIMIT, TimeInForce.POST_ONLY, 2L, 2L));
        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(0, commands.size());
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(commands, "", 1, buffer);

        List<Command> view = AeronReplicatedCommandBatchCodec.decode(buffer, 0, encodedLength).commands();
        List<Long> sequences = new ArrayList<>();
        for (Command command : view) {
            sequences.add(command.sequence);
        }

        assertEquals(List.of(10L, 11L), sequences);
    }

    @Test
    void roundTripsSingleCommandBatch() {
        List<Command> commands = List.of(Command.cancel(7L, 8L, 9));

        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 1);
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(commands, "", 0, buffer);

        AeronReplicatedCommandBatchCodec.DecodedBatch decoded =
                AeronReplicatedCommandBatchCodec.decode(buffer, 0, encodedLength);

        assertEquals(1, decoded.commands().size());
        assertEquals(CommandType.CANCEL_ORDER, decoded.commands().get(0).type);
        assertEquals("", decoded.responseChannel());
    }

    @Test
    void encodedLengthGrowsByOneFixedCommandPerEntry() {
        assertEquals(Integer.BYTES * 4, AeronReplicatedCommandBatchCodec.encodedLength(0, 0));
        assertEquals(AeronReplicatedCommandBatchCodec.encodedLength(0, 0) + AeronCommandCodec.ENCODED_LENGTH,
                AeronReplicatedCommandBatchCodec.encodedLength(0, 1));
        assertEquals(AeronReplicatedCommandBatchCodec.encodedLength(0, 2) + 5,
                AeronReplicatedCommandBatchCodec.encodedLength(5, 2));
    }

    @Test
    void decodesFromNonZeroOffset() {
        List<Command> commands = List.of(Command.shutdown(1L), Command.shutdown(2L));
        String responseChannel = "aeron:ipc";
        UnsafeBuffer encoded = AeronReplicatedCommandBatchCodec.allocateBuffer(
                responseChannel.length(), commands.size());
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(commands, responseChannel, 4, encoded);

        int offset = 16;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        AeronReplicatedCommandBatchCodec.DecodedBatch decoded =
                AeronReplicatedCommandBatchCodec.decode(framed, offset, encodedLength);

        assertEquals(responseChannel, decoded.responseChannel());
        assertEquals(4, decoded.responseStreamId());
        assertEquals(2L, decoded.commands().get(1).sequence);
    }

    @Test
    void decodedCommandListRejectsOutOfRangeIndexes() {
        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 1);
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(
                List.of(Command.shutdown(1L)), "", 0, buffer);

        List<Command> view = AeronReplicatedCommandBatchCodec.decode(buffer, 0, encodedLength).commands();

        assertThrows(IndexOutOfBoundsException.class, () -> view.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> view.get(1));
    }

    @Test
    void rejectsNullCommandList() {
        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 1);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandBatchCodec.encode(null, "", 0, buffer));

        assertTrue(failure.getMessage().contains("commands must not be empty"));
    }

    @Test
    void rejectsEmptyCommandList() {
        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 1);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandBatchCodec.encode(List.of(), "", 0, buffer));

        assertTrue(failure.getMessage().contains("commands must not be empty"));
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        List<Command> commands = List.of(Command.shutdown(1L), Command.shutdown(2L));
        UnsafeBuffer tooSmall = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 1);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandBatchCodec.encode(commands, "", 0, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsForeignFrameKind() {
        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 1);
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(
                List.of(Command.shutdown(1L)), "", 0, buffer);
        buffer.putInt(0, AeronReplicatedCommandCodec.FRAME_KIND_SINGLE);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandBatchCodec.decode(buffer, 0, encodedLength));

        assertTrue(failure.getMessage().contains("unsupported replicated command batch frame kind"));
    }

    @Test
    void decodedCommandListFailsWhenTheFrameIsTruncated() {
        UnsafeBuffer encoded = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 2);
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(
                List.of(Command.shutdown(1L), Command.shutdown(2L)), "", 0, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - AeronCommandCodec.ENCODED_LENGTH]);
        truncated.putBytes(0, encoded, 0, encodedLength - AeronCommandCodec.ENCODED_LENGTH);

        assertThrows(IllegalArgumentException.class, () -> AeronReplicatedCommandBatchCodec
                .decode(truncated, 0, encodedLength - AeronCommandCodec.ENCODED_LENGTH));
    }

    @Test
    void rejectsCommandCountThatWouldWalkPastTheFrame() {
        UnsafeBuffer buffer = AeronReplicatedCommandBatchCodec.allocateBuffer(0, 1);
        int encodedLength = AeronReplicatedCommandBatchCodec.encode(List.of(Command.shutdown(1L)), "", 0, buffer);
        buffer.putInt(8, Integer.MAX_VALUE);

        assertThrows(IllegalArgumentException.class,
                () -> AeronReplicatedCommandBatchCodec.decode(buffer, 0, encodedLength));
    }
}
