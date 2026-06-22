package io.github.ike.ullmatcher.ha.aeron;

import io.github.ike.ullmatcher.api.Command;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 编解码带直接确认返回地址的复制命令。
 */
public final class AeronReplicatedCommandCodec {
    public static final int FRAME_KIND_SINGLE = 1;

    private static final int OFFSET_FRAME_KIND = 0;
    private static final int OFFSET_RESPONSE_STREAM_ID = OFFSET_FRAME_KIND + Integer.BYTES;
    private static final int OFFSET_CHANNEL_LENGTH = OFFSET_RESPONSE_STREAM_ID + Integer.BYTES;
    private static final int OFFSET_CHANNEL = OFFSET_CHANNEL_LENGTH + Integer.BYTES;

    private AeronReplicatedCommandCodec() {
    }

    public static UnsafeBuffer allocateBuffer(int responseChannelLength) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength(responseChannelLength)));
    }

    public static int encodedLength(int responseChannelLength) {
        return OFFSET_CHANNEL + responseChannelLength + AeronCommandCodec.ENCODED_LENGTH;
    }

    public static int encode(Command command, String responseChannel, int responseStreamId, MutableDirectBuffer buffer) {
        byte[] responseChannelBytes = responseChannel.getBytes(StandardCharsets.UTF_8);
        int encodedLength = encodedLength(responseChannelBytes.length);
        if (buffer.capacity() < encodedLength) {
            throw new IllegalArgumentException("buffer capacity too small for replicated command");
        }
        buffer.putInt(OFFSET_FRAME_KIND, FRAME_KIND_SINGLE);
        buffer.putInt(OFFSET_RESPONSE_STREAM_ID, responseStreamId);
        buffer.putInt(OFFSET_CHANNEL_LENGTH, responseChannelBytes.length);
        buffer.putBytes(OFFSET_CHANNEL, responseChannelBytes);
        UnsafeBuffer commandBuffer = AeronCommandCodec.allocateBuffer();
        AeronCommandCodec.encode(command, commandBuffer);
        buffer.putBytes(OFFSET_CHANNEL + responseChannelBytes.length, commandBuffer, 0, AeronCommandCodec.ENCODED_LENGTH);
        return encodedLength;
    }

    public static DecodedCommand decode(DirectBuffer buffer, int offset, int length) {
        int frameKind = buffer.getInt(offset + OFFSET_FRAME_KIND);
        if (frameKind != FRAME_KIND_SINGLE) {
            throw new IllegalArgumentException("unsupported replicated command frame kind " + frameKind);
        }
        int responseStreamId = buffer.getInt(offset + OFFSET_RESPONSE_STREAM_ID);
        int channelLength = buffer.getInt(offset + OFFSET_CHANNEL_LENGTH);
        byte[] channelBytes = new byte[channelLength];
        buffer.getBytes(offset + OFFSET_CHANNEL, channelBytes);
        int commandOffset = offset + OFFSET_CHANNEL + channelLength;
        Command command = AeronCommandCodec.decode(buffer, commandOffset);
        return new DecodedCommand(command, new String(channelBytes, StandardCharsets.UTF_8), responseStreamId);
    }

    public static int frameKind(DirectBuffer buffer, int offset) {
        return buffer.getInt(offset + OFFSET_FRAME_KIND);
    }

    public record DecodedCommand(Command command, String responseChannel, int responseStreamId) {
    }
}
