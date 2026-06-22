package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * 对快照分片进行顺序二进制编解码。
 */
public final class AeronSnapshotChunkCodec {
    public static final int HEADER_LENGTH = (Long.BYTES * 5) + (Integer.BYTES * 3);
    public static final int MAX_CHUNK_BYTES = 60 * 1024;
    private static final int FLAG_LAST_CHUNK = 1;

    private static final int OFFSET_REQUEST_ID = 0;
    private static final int OFFSET_LAST_SEQUENCE = OFFSET_REQUEST_ID + Long.BYTES;
    private static final int OFFSET_LAST_TRADE_ID = OFFSET_LAST_SEQUENCE + Long.BYTES;
    private static final int OFFSET_LIVE_ORDER_COUNT = OFFSET_LAST_TRADE_ID + Long.BYTES;
    private static final int OFFSET_TOTAL_BYTES = OFFSET_LIVE_ORDER_COUNT + Long.BYTES;
    private static final int OFFSET_CHUNK_INDEX = OFFSET_TOTAL_BYTES + Long.BYTES;
    private static final int OFFSET_FLAGS = OFFSET_CHUNK_INDEX + Integer.BYTES;
    private static final int OFFSET_PAYLOAD_LENGTH = OFFSET_FLAGS + Integer.BYTES;
    private static final int OFFSET_PAYLOAD = OFFSET_PAYLOAD_LENGTH + Integer.BYTES;

    private AeronSnapshotChunkCodec() {
    }

    public static UnsafeBuffer allocateBuffer() {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(HEADER_LENGTH + MAX_CHUNK_BYTES));
    }

    public static int encode(Chunk chunk, byte[] payload, int payloadLength, MutableDirectBuffer buffer) {
        if (payloadLength < 0 || payloadLength > MAX_CHUNK_BYTES) {
            throw new IllegalArgumentException("invalid snapshot payload length " + payloadLength);
        }
        buffer.putLong(OFFSET_REQUEST_ID, chunk.requestId());
        buffer.putLong(OFFSET_LAST_SEQUENCE, chunk.lastSequence());
        buffer.putLong(OFFSET_LAST_TRADE_ID, chunk.lastTradeId());
        buffer.putLong(OFFSET_LIVE_ORDER_COUNT, chunk.liveOrderCount());
        buffer.putLong(OFFSET_TOTAL_BYTES, chunk.totalBytes());
        buffer.putInt(OFFSET_CHUNK_INDEX, chunk.chunkIndex());
        buffer.putInt(OFFSET_FLAGS, chunk.lastChunk() ? FLAG_LAST_CHUNK : 0);
        buffer.putInt(OFFSET_PAYLOAD_LENGTH, payloadLength);
        buffer.putBytes(OFFSET_PAYLOAD, payload, 0, payloadLength);
        return OFFSET_PAYLOAD + payloadLength;
    }

    public static DecodedChunk decode(DirectBuffer buffer, int offset, int length) {
        long requestId = buffer.getLong(offset + OFFSET_REQUEST_ID);
        long lastSequence = buffer.getLong(offset + OFFSET_LAST_SEQUENCE);
        long lastTradeId = buffer.getLong(offset + OFFSET_LAST_TRADE_ID);
        long liveOrderCount = buffer.getLong(offset + OFFSET_LIVE_ORDER_COUNT);
        long totalBytes = buffer.getLong(offset + OFFSET_TOTAL_BYTES);
        int chunkIndex = buffer.getInt(offset + OFFSET_CHUNK_INDEX);
        boolean lastChunk = (buffer.getInt(offset + OFFSET_FLAGS) & FLAG_LAST_CHUNK) != 0;
        int payloadLength = buffer.getInt(offset + OFFSET_PAYLOAD_LENGTH);
        byte[] payload = new byte[payloadLength];
        buffer.getBytes(offset + OFFSET_PAYLOAD, payload);
        return new DecodedChunk(
                new Chunk(requestId, lastSequence, lastTradeId, liveOrderCount, totalBytes, chunkIndex, lastChunk),
                payload
        );
    }

    public record Chunk(
            long requestId,
            long lastSequence,
            long lastTradeId,
            long liveOrderCount,
            long totalBytes,
            int chunkIndex,
            boolean lastChunk
    ) {
    }

    public record DecodedChunk(Chunk chunk, byte[] payload) {
    }
}
