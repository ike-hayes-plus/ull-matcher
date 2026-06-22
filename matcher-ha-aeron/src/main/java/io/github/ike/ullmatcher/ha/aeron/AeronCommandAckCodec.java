package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * 编解码复制命令的直接确认帧。
 */
public final class AeronCommandAckCodec {
    public static final int ENCODED_LENGTH = Long.BYTES;

    private AeronCommandAckCodec() {
    }

    public static UnsafeBuffer allocateBuffer() {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(ENCODED_LENGTH));
    }

    public static int encode(long sequence, MutableDirectBuffer buffer) {
        buffer.putLong(0, sequence);
        return ENCODED_LENGTH;
    }

    public static long decode(DirectBuffer buffer, int offset) {
        return buffer.getLong(offset);
    }
}
