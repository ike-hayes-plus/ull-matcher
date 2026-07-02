package io.github.ike.ullmatcher.ha.aeron;

import io.aeron.logbuffer.Header;
import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandType;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * 对复制命令进行定长二进制编解码。
 */
public final class AeronCommandCodec {
    public static final int ENCODED_LENGTH = (Integer.BYTES * 5) + (Long.BYTES * 6);

    private static final int OFFSET_TYPE = 0;
    private static final int OFFSET_SEQUENCE = OFFSET_TYPE + Integer.BYTES;
    private static final int OFFSET_ORDER_ID = OFFSET_SEQUENCE + Long.BYTES;
    private static final int OFFSET_USER_ID = OFFSET_ORDER_ID + Long.BYTES;
    private static final int OFFSET_SYMBOL_ID = OFFSET_USER_ID + Long.BYTES;
    private static final int OFFSET_SIDE = OFFSET_SYMBOL_ID + Integer.BYTES;
    private static final int OFFSET_ORDER_TYPE = OFFSET_SIDE + Integer.BYTES;
    private static final int OFFSET_TIME_IN_FORCE = OFFSET_ORDER_TYPE + Integer.BYTES;
    private static final int OFFSET_PRICE = OFFSET_TIME_IN_FORCE + Integer.BYTES;
    private static final int OFFSET_QUANTITY = OFFSET_PRICE + Long.BYTES;
    private static final int OFFSET_EXPIRE_AT = OFFSET_QUANTITY + Long.BYTES;

    private AeronCommandCodec() {}

    public static UnsafeBuffer allocateBuffer() {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODED_LENGTH));
    }

    public static int encode(Command command, MutableDirectBuffer buffer) {
        buffer.putInt(OFFSET_TYPE, command.type.ordinal());
        buffer.putLong(OFFSET_SEQUENCE, command.sequence);
        buffer.putLong(OFFSET_ORDER_ID, command.orderId);
        buffer.putLong(OFFSET_USER_ID, command.userId);
        buffer.putInt(OFFSET_SYMBOL_ID, command.symbolId);
        buffer.putInt(OFFSET_SIDE, command.side);
        buffer.putInt(OFFSET_ORDER_TYPE, command.orderType);
        buffer.putInt(OFFSET_TIME_IN_FORCE, command.timeInForce);
        buffer.putLong(OFFSET_PRICE, command.price);
        buffer.putLong(OFFSET_QUANTITY, command.quantity);
        buffer.putLong(OFFSET_EXPIRE_AT, command.expireAtEpochMillis);
        return ENCODED_LENGTH;
    }

    public static Command decode(DirectBuffer buffer, int offset) {
        CommandType type = CommandType.values()[buffer.getInt(offset + OFFSET_TYPE)];
        long sequence = buffer.getLong(offset + OFFSET_SEQUENCE);
        long orderId = buffer.getLong(offset + OFFSET_ORDER_ID);
        long userId = buffer.getLong(offset + OFFSET_USER_ID);
        int symbolId = buffer.getInt(offset + OFFSET_SYMBOL_ID);
        byte side = (byte) buffer.getInt(offset + OFFSET_SIDE);
        byte orderType = (byte) buffer.getInt(offset + OFFSET_ORDER_TYPE);
        byte timeInForce = (byte) buffer.getInt(offset + OFFSET_TIME_IN_FORCE);
        long price = buffer.getLong(offset + OFFSET_PRICE);
        long quantity = buffer.getLong(offset + OFFSET_QUANTITY);
        long expireAtEpochMillis = buffer.getLong(offset + OFFSET_EXPIRE_AT);
        return switch (type) {
            case NEW_ORDER -> Command.newOrder(
                    sequence,
                    orderId,
                    userId,
                    symbolId,
                    side(side),
                    orderType(orderType),
                    timeInForce(timeInForce),
                    price,
                    quantity,
                    expireAtEpochMillis
            );
            case CANCEL_ORDER -> Command.cancel(sequence, orderId, symbolId);
            case SNAPSHOT_MARKER -> Command.snapshotMarker(sequence, symbolId);
            case SHUTDOWN -> Command.shutdown(sequence);
        };
    }

    public static Command decode(DirectBuffer buffer, int offset, int length, Header header) {
        return decode(buffer, offset);
    }

    private static Side side(byte code) {
        for (Side value : Side.values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown side code: " + code);
    }

    private static OrderType orderType(byte code) {
        for (OrderType value : OrderType.values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown orderType code: " + code);
    }

    private static TimeInForce timeInForce(byte code) {
        for (TimeInForce value : TimeInForce.values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown timeInForce code: " + code);
    }
}
