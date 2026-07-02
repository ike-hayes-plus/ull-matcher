package io.github.ike.ullmatcher.ha.aeron;

import io.github.ike.ullmatcher.api.Command;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.List;

/**
 * 编解码批量复制命令帧。
 */
public final class AeronReplicatedCommandBatchCodec {
    public static final int FRAME_KIND_BATCH = 2;

    private static final int OFFSET_FRAME_KIND = 0;
    private static final int OFFSET_RESPONSE_STREAM_ID = OFFSET_FRAME_KIND + Integer.BYTES;
    private static final int OFFSET_COMMAND_COUNT = OFFSET_RESPONSE_STREAM_ID + Integer.BYTES;
    private static final int OFFSET_CHANNEL_LENGTH = OFFSET_COMMAND_COUNT + Integer.BYTES;
    private static final int OFFSET_CHANNEL = OFFSET_CHANNEL_LENGTH + Integer.BYTES;

    private AeronReplicatedCommandBatchCodec() {
    }

    public static UnsafeBuffer allocateBuffer(int responseChannelLength, int commandCount) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength(responseChannelLength, commandCount)));
    }

    public static int encodedLength(int responseChannelLength, int commandCount) {
        return OFFSET_CHANNEL + responseChannelLength + (commandCount * AeronCommandCodec.ENCODED_LENGTH);
    }

    public static int encode(List<Command> commands, String responseChannel, int responseStreamId, MutableDirectBuffer buffer) {
        if (commands == null || commands.isEmpty()) {
            throw new IllegalArgumentException("commands must not be empty");
        }
        byte[] responseChannelBytes = responseChannel.getBytes(StandardCharsets.UTF_8);
        int encodedLength = encodedLength(responseChannelBytes.length, commands.size());
        if (buffer.capacity() < encodedLength) {
            throw new IllegalArgumentException("buffer capacity too small for replicated command batch");
        }
        buffer.putInt(OFFSET_FRAME_KIND, FRAME_KIND_BATCH);
        buffer.putInt(OFFSET_RESPONSE_STREAM_ID, responseStreamId);
        buffer.putInt(OFFSET_COMMAND_COUNT, commands.size());
        buffer.putInt(OFFSET_CHANNEL_LENGTH, responseChannelBytes.length);
        buffer.putBytes(OFFSET_CHANNEL, responseChannelBytes);
        int commandOffset = OFFSET_CHANNEL + responseChannelBytes.length;
        for (Command command : commands) {
            UnsafeBuffer commandBuffer = AeronCommandCodec.allocateBuffer();
            AeronCommandCodec.encode(command, commandBuffer);
            buffer.putBytes(commandOffset, commandBuffer, 0, AeronCommandCodec.ENCODED_LENGTH);
            commandOffset += AeronCommandCodec.ENCODED_LENGTH;
        }
        return encodedLength;
    }

    public static DecodedBatch decode(DirectBuffer buffer, int offset, int length) {
        int frameKind = buffer.getInt(offset + OFFSET_FRAME_KIND);
        if (frameKind != FRAME_KIND_BATCH) {
            throw new IllegalArgumentException("unsupported replicated command batch frame kind " + frameKind);
        }
        int responseStreamId = buffer.getInt(offset + OFFSET_RESPONSE_STREAM_ID);
        int commandCount = buffer.getInt(offset + OFFSET_COMMAND_COUNT);
        int channelLength = buffer.getInt(offset + OFFSET_CHANNEL_LENGTH);
        byte[] channelBytes = new byte[channelLength];
        buffer.getBytes(offset + OFFSET_CHANNEL, channelBytes);
        int commandsOffset = offset + OFFSET_CHANNEL + channelLength;
        return new DecodedBatch(
                new CommandListView(buffer, commandsOffset, commandCount),
                new String(channelBytes, StandardCharsets.UTF_8),
                responseStreamId
        );
    }

    public record DecodedBatch(List<Command> commands, String responseChannel, int responseStreamId) {
    }

    private static final class CommandListView extends AbstractList<Command> {
        private final DirectBuffer buffer;
        private final int commandsOffset;
        private final int commandCount;

        private CommandListView(DirectBuffer buffer, int commandsOffset, int commandCount) {
            this.buffer = buffer;
            this.commandsOffset = commandsOffset;
            this.commandCount = commandCount;
        }

        @Override
        public Command get(int index) {
            if (index < 0 || index >= commandCount) {
                throw new IndexOutOfBoundsException(index);
            }
            return AeronCommandCodec.decode(buffer, commandsOffset + (index * AeronCommandCodec.ENCODED_LENGTH));
        }

        @Override
        public int size() {
            return commandCount;
        }
    }
}
