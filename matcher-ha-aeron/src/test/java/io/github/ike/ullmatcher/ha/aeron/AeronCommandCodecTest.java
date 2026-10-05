package io.github.ike.ullmatcher.ha.aeron;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandType;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.github.ike.ullmatcher.ha.aeron.AeronReplicatedCommandCodecTest.assertCommandEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the fixed-length replication command frame. The enum codes are written as ints
 * but narrowed back to bytes on decode, so an unmapped code must fail loudly instead of silently
 * replaying a different order.
 */
final class AeronCommandCodecTest {
    private static final int OFFSET_SIDE = (Integer.BYTES * 2) + (Long.BYTES * 3);
    private static final int OFFSET_ORDER_TYPE = OFFSET_SIDE + Integer.BYTES;
    private static final int OFFSET_TIME_IN_FORCE = OFFSET_ORDER_TYPE + Integer.BYTES;

    @Test
    void encodedLengthMatchesTheDeclaredFieldLayout() {
        assertEquals((Integer.BYTES * 5) + (Long.BYTES * 6), AeronCommandCodec.ENCODED_LENGTH);
        assertEquals(AeronCommandCodec.ENCODED_LENGTH, AeronCommandCodec.allocateBuffer().capacity());
    }

    @Test
    void roundTripsEveryEnumCombinationOfANewOrder() {
        for (Side side : Side.values()) {
            for (OrderType orderType : OrderType.values()) {
                for (TimeInForce timeInForce : TimeInForce.values()) {
                    Command command = Command.newOrder(
                            1L, 2L, 3L, 4, side, orderType, timeInForce, 500L, 600L, 700L);

                    UnsafeBuffer buffer = AeronCommandCodec.allocateBuffer();
                    assertEquals(AeronCommandCodec.ENCODED_LENGTH, AeronCommandCodec.encode(command, buffer));

                    Command decoded = AeronCommandCodec.decode(buffer, 0);

                    assertCommandEquals(command, decoded);
                    assertEquals(side.code, decoded.side);
                    assertEquals(orderType.code, decoded.orderType);
                    assertEquals(timeInForce.code, decoded.timeInForce);
                }
            }
        }
    }

    @Test
    void roundTripsCancelOrder() {
        Command command = Command.cancel(Long.MAX_VALUE, 4242L, Integer.MAX_VALUE);

        UnsafeBuffer buffer = AeronCommandCodec.allocateBuffer();
        AeronCommandCodec.encode(command, buffer);
        Command decoded = AeronCommandCodec.decode(buffer, 0);

        assertEquals(CommandType.CANCEL_ORDER, decoded.type);
        assertCommandEquals(command, decoded);
    }

    @Test
    void roundTripsSnapshotMarker() {
        Command command = Command.snapshotMarker(77L, 13);

        UnsafeBuffer buffer = AeronCommandCodec.allocateBuffer();
        AeronCommandCodec.encode(command, buffer);
        Command decoded = AeronCommandCodec.decode(buffer, 0);

        assertEquals(CommandType.SNAPSHOT_MARKER, decoded.type);
        assertCommandEquals(command, decoded);
    }

    @Test
    void roundTripsShutdown() {
        Command command = Command.shutdown(Long.MIN_VALUE);

        UnsafeBuffer buffer = AeronCommandCodec.allocateBuffer();
        AeronCommandCodec.encode(command, buffer);
        Command decoded = AeronCommandCodec.decode(buffer, 0);

        assertEquals(CommandType.SHUTDOWN, decoded.type);
        assertCommandEquals(command, decoded);
    }

    @Test
    void roundTripsExtremeNumericValues() {
        Command command = Command.newOrder(
                Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Integer.MIN_VALUE,
                Side.SELL, OrderType.MARKET_WITH_PROTECTION, TimeInForce.POST_ONLY,
                Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);

        UnsafeBuffer buffer = AeronCommandCodec.allocateBuffer();
        AeronCommandCodec.encode(command, buffer);

        assertCommandEquals(command, AeronCommandCodec.decode(buffer, 0));
    }

    @Test
    void decodesFromNonZeroOffsetAndThroughTheFragmentHandlerOverload() {
        Command command = Command.newOrder(
                9L, 8L, 7L, 6, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 5L, 4L, 3L);
        UnsafeBuffer encoded = AeronCommandCodec.allocateBuffer();
        AeronCommandCodec.encode(command, encoded);

        int offset = 32;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + AeronCommandCodec.ENCODED_LENGTH]);
        framed.putBytes(offset, encoded, 0, AeronCommandCodec.ENCODED_LENGTH);

        assertCommandEquals(command, AeronCommandCodec.decode(framed, offset));
        assertCommandEquals(command,
                AeronCommandCodec.decode(framed, offset, AeronCommandCodec.ENCODED_LENGTH, null));
    }

    @Test
    void rejectsUnknownSideCode() {
        UnsafeBuffer buffer = newOrderBuffer();
        buffer.putInt(OFFSET_SIDE, 99);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronCommandCodec.decode(buffer, 0));

        assertTrue(failure.getMessage().contains("unknown side code"));
    }

    @Test
    void rejectsUnknownOrderTypeCode() {
        UnsafeBuffer buffer = newOrderBuffer();
        buffer.putInt(OFFSET_ORDER_TYPE, 99);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronCommandCodec.decode(buffer, 0));

        assertTrue(failure.getMessage().contains("unknown orderType code"));
    }

    @Test
    void rejectsUnknownTimeInForceCode() {
        UnsafeBuffer buffer = newOrderBuffer();
        buffer.putInt(OFFSET_TIME_IN_FORCE, 99);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronCommandCodec.decode(buffer, 0));

        assertTrue(failure.getMessage().contains("unknown timeInForce code"));
    }

    @Test
    void rejectsUnknownCommandTypeOrdinal() {
        UnsafeBuffer buffer = newOrderBuffer();
        buffer.putInt(0, CommandType.values().length);

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> AeronCommandCodec.decode(buffer, 0));
    }

    @Test
    void rejectsTruncatedFrame() {
        UnsafeBuffer encoded = newOrderBuffer();
        UnsafeBuffer truncated = new UnsafeBuffer(new byte[AeronCommandCodec.ENCODED_LENGTH - 1]);
        truncated.putBytes(0, encoded, 0, AeronCommandCodec.ENCODED_LENGTH - 1);

        assertThrows(IndexOutOfBoundsException.class, () -> AeronCommandCodec.decode(truncated, 0));
    }

    private static UnsafeBuffer newOrderBuffer() {
        Command command = Command.newOrder(
                1L, 2L, 3L, 4, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 10L, 20L, 30L);
        UnsafeBuffer buffer = AeronCommandCodec.allocateBuffer();
        AeronCommandCodec.encode(command, buffer);
        return buffer;
    }
}
