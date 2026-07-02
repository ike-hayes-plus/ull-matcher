package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 编解码基于 Aeron 的快照拉取请求。
 */
public final class AeronSnapshotRequestCodec {
    private static final int OFFSET_REQUEST_ID = 0;
    private static final int OFFSET_SESSION_ID = OFFSET_REQUEST_ID + Long.BYTES;
    private static final int OFFSET_RESPONSE_STREAM_ID = OFFSET_SESSION_ID + Long.BYTES;
    private static final int OFFSET_CHANNEL_LENGTH = OFFSET_RESPONSE_STREAM_ID + Integer.BYTES;
    private static final int OFFSET_CHANNEL = OFFSET_CHANNEL_LENGTH + Integer.BYTES;

    private AeronSnapshotRequestCodec() {
    }

    public static UnsafeBuffer allocateBuffer(int channelLength) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength(channelLength)));
    }

    public static int encodedLength(int channelLength) {
        return OFFSET_CHANNEL + channelLength;
    }

    public static int encode(long requestId, long sessionId, String responseChannel, int responseStreamId, MutableDirectBuffer buffer) {
        byte[] channelBytes = responseChannel.getBytes(StandardCharsets.UTF_8);
        if (buffer.capacity() < encodedLength(channelBytes.length)) {
            throw new IllegalArgumentException("buffer capacity too small for snapshot request");
        }
        buffer.putLong(OFFSET_REQUEST_ID, requestId);
        buffer.putLong(OFFSET_SESSION_ID, sessionId);
        buffer.putInt(OFFSET_RESPONSE_STREAM_ID, responseStreamId);
        buffer.putInt(OFFSET_CHANNEL_LENGTH, channelBytes.length);
        buffer.putBytes(OFFSET_CHANNEL, channelBytes);
        return encodedLength(channelBytes.length);
    }

    public static Request decode(DirectBuffer buffer, int offset, int length) {
        long requestId = buffer.getLong(offset + OFFSET_REQUEST_ID);
        long sessionId = buffer.getLong(offset + OFFSET_SESSION_ID);
        int responseStreamId = buffer.getInt(offset + OFFSET_RESPONSE_STREAM_ID);
        int channelLength = buffer.getInt(offset + OFFSET_CHANNEL_LENGTH);
        byte[] channelBytes = new byte[channelLength];
        buffer.getBytes(offset + OFFSET_CHANNEL, channelBytes);
        return new Request(requestId, sessionId, new String(channelBytes, StandardCharsets.UTF_8), responseStreamId);
    }

    public record Request(long requestId, long sessionId, String responseChannel, int responseStreamId) {
    }
}
