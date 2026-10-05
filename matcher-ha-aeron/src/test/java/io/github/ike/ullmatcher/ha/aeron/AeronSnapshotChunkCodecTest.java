package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for snapshot transfer chunks. The payload length is attacker-controlled on the
 * receive side, so encode and decode both refuse anything outside {@code [0, MAX_CHUNK_BYTES]}.
 */
final class AeronSnapshotChunkCodecTest {
    @Test
    void headerLengthMatchesTheDeclaredFieldLayout() {
        assertEquals((Long.BYTES * 5) + (Integer.BYTES * 3), AeronSnapshotChunkCodec.HEADER_LENGTH);
        assertEquals(AeronSnapshotChunkCodec.HEADER_LENGTH + AeronSnapshotChunkCodec.MAX_CHUNK_BYTES,
                AeronSnapshotChunkCodec.allocateBuffer().capacity());
    }

    @Test
    void roundTripsEveryFieldOfAnIntermediateChunk() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                0x0A0B0C0D0E0F1011L, 9_000L, 8_000L, 7_000L, 6_000L, 3, false);
        byte[] payload = {1, 2, 3, 4, 5};

        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, payload, payload.length, buffer);

        assertEquals(AeronSnapshotChunkCodec.HEADER_LENGTH + payload.length, encodedLength);

        AeronSnapshotChunkCodec.DecodedChunk decoded =
                AeronSnapshotChunkCodec.decode(buffer, 0, encodedLength);

        assertEquals(chunk, decoded.chunk());
        assertFalse(decoded.chunk().lastChunk());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void lastChunkFlagRoundTrips() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 2L, 3L, 4L, 5L, 6, true);

        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, new byte[]{7}, 1, buffer);

        AeronSnapshotChunkCodec.DecodedChunk decoded =
                AeronSnapshotChunkCodec.decode(buffer, 0, encodedLength);

        assertTrue(decoded.chunk().lastChunk());
        assertEquals(chunk, decoded.chunk());
    }

    @Test
    void acceptsEmptyPayload() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 0L, 0L, 0L, 0L, 0, true);

        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, new byte[0], 0, buffer);

        assertEquals(AeronSnapshotChunkCodec.HEADER_LENGTH, encodedLength);
        assertArrayEquals(new byte[0], AeronSnapshotChunkCodec.decode(buffer, 0, encodedLength).payload());
    }

    @Test
    void acceptsMaximumPayload() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 2L, 3L, 4L, AeronSnapshotChunkCodec.MAX_CHUNK_BYTES, 0, false);
        byte[] payload = new byte[AeronSnapshotChunkCodec.MAX_CHUNK_BYTES];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, payload, payload.length, buffer);

        assertEquals(buffer.capacity(), encodedLength);
        assertArrayEquals(payload, AeronSnapshotChunkCodec.decode(buffer, 0, encodedLength).payload());
    }

    @Test
    void encodesOnlyThePrefixRequestedByPayloadLength() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 2L, 3L, 4L, 5L, 0, false);
        byte[] payload = {1, 2, 3, 4, 5, 6, 7, 8};

        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, payload, 3, buffer);

        assertArrayEquals(new byte[]{1, 2, 3},
                AeronSnapshotChunkCodec.decode(buffer, 0, encodedLength).payload());
    }

    @Test
    void roundTripsExtremeNumericValues() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                Integer.MAX_VALUE, true);

        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, new byte[0], 0, buffer);

        assertEquals(chunk, AeronSnapshotChunkCodec.decode(buffer, 0, encodedLength).chunk());
    }

    @Test
    void decodesFromNonZeroOffset() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                11L, 12L, 13L, 14L, 15L, 16, true);
        byte[] payload = {9, 9, 9};

        UnsafeBuffer encoded = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, payload, payload.length, encoded);

        int offset = 8;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        AeronSnapshotChunkCodec.DecodedChunk decoded =
                AeronSnapshotChunkCodec.decode(framed, offset, encodedLength);

        assertEquals(chunk, decoded.chunk());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void rejectsNegativePayloadLength() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 2L, 3L, 4L, 5L, 0, false);
        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSnapshotChunkCodec.encode(chunk, new byte[4], -1, buffer));

        assertTrue(failure.getMessage().contains("invalid snapshot payload length"));
    }

    @Test
    void rejectsPayloadLengthAboveTheChunkLimit() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 2L, 3L, 4L, 5L, 0, false);
        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        byte[] payload = new byte[AeronSnapshotChunkCodec.MAX_CHUNK_BYTES + 1];

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSnapshotChunkCodec.encode(chunk, payload, payload.length, buffer));

        assertTrue(failure.getMessage().contains("invalid snapshot payload length"));
    }

    @Test
    void rejectsTruncatedFrame() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 2L, 3L, 4L, 5L, 0, false);
        UnsafeBuffer encoded = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, new byte[]{1, 2, 3, 4}, 4, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 2]);
        truncated.putBytes(0, encoded, 0, encodedLength - 2);

        assertThrows(IllegalArgumentException.class,
                () -> AeronSnapshotChunkCodec.decode(truncated, 0, encodedLength - 2));
    }

    @Test
    void rejectsPayloadLengthThatWouldAllocatePastTheFrame() {
        AeronSnapshotChunkCodec.Chunk chunk = new AeronSnapshotChunkCodec.Chunk(
                1L, 2L, 3L, 4L, 5L, 0, false);
        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int encodedLength = AeronSnapshotChunkCodec.encode(chunk, new byte[]{1}, 1, buffer);
        buffer.putInt(48, Integer.MAX_VALUE);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSnapshotChunkCodec.decode(buffer, 0, encodedLength));

        assertTrue(failure.getMessage().contains("exceeds frame"));
    }
}
