package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the snapshot pull request frame.
 */
final class AeronSnapshotRequestCodecTest {
    @Test
    void roundTripsEveryField() {
        String responseChannel = "aeron:udp?endpoint=127.0.0.1:20701";
        int channelLength = responseChannel.getBytes(StandardCharsets.UTF_8).length;

        UnsafeBuffer buffer = AeronSnapshotRequestCodec.allocateBuffer(channelLength);
        int encodedLength = AeronSnapshotRequestCodec.encode(
                987_654_321L, -123_456_789L, responseChannel, 4711, buffer);

        assertEquals(AeronSnapshotRequestCodec.encodedLength(channelLength), encodedLength);
        assertEquals(buffer.capacity(), encodedLength);

        assertEquals(new AeronSnapshotRequestCodec.Request(987_654_321L, -123_456_789L, responseChannel, 4711),
                AeronSnapshotRequestCodec.decode(buffer, 0, encodedLength));
    }

    @Test
    void encodedLengthIsTheFixedHeaderPlusChannelBytes() {
        assertEquals(Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES,
                AeronSnapshotRequestCodec.encodedLength(0));
        assertEquals(AeronSnapshotRequestCodec.encodedLength(0) + 7,
                AeronSnapshotRequestCodec.encodedLength(7));
    }

    @Test
    void roundTripsEmptyChannelAndExtremeIdentifiers() {
        UnsafeBuffer buffer = AeronSnapshotRequestCodec.allocateBuffer(0);
        int encodedLength = AeronSnapshotRequestCodec.encode(
                Long.MAX_VALUE, Long.MIN_VALUE, "", Integer.MAX_VALUE, buffer);

        AeronSnapshotRequestCodec.Request decoded = AeronSnapshotRequestCodec.decode(buffer, 0, encodedLength);

        assertEquals(Long.MAX_VALUE, decoded.requestId());
        assertEquals(Long.MIN_VALUE, decoded.sessionId());
        assertEquals("", decoded.responseChannel());
        assertEquals(Integer.MAX_VALUE, decoded.responseStreamId());
    }

    @Test
    void roundTripsMultiByteChannel() {
        String responseChannel = "aeron:ipc?别名=快照";
        int channelLength = responseChannel.getBytes(StandardCharsets.UTF_8).length;

        UnsafeBuffer buffer = AeronSnapshotRequestCodec.allocateBuffer(channelLength);
        int encodedLength = AeronSnapshotRequestCodec.encode(1L, 2L, responseChannel, 3, buffer);

        assertEquals(responseChannel,
                AeronSnapshotRequestCodec.decode(buffer, 0, encodedLength).responseChannel());
    }

    @Test
    void decodesFromNonZeroOffset() {
        String responseChannel = "aeron:ipc";
        UnsafeBuffer encoded = AeronSnapshotRequestCodec.allocateBuffer(responseChannel.length());
        int encodedLength = AeronSnapshotRequestCodec.encode(8L, 9L, responseChannel, 10, encoded);

        int offset = 12;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        assertEquals(new AeronSnapshotRequestCodec.Request(8L, 9L, responseChannel, 10),
                AeronSnapshotRequestCodec.decode(framed, offset, encodedLength));
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        UnsafeBuffer tooSmall = AeronSnapshotRequestCodec.allocateBuffer(2);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSnapshotRequestCodec.encode(1L, 2L, "aeron:ipc", 3, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsTruncatedFrame() {
        String responseChannel = "aeron:ipc";
        UnsafeBuffer encoded = AeronSnapshotRequestCodec.allocateBuffer(responseChannel.length());
        int encodedLength = AeronSnapshotRequestCodec.encode(1L, 2L, responseChannel, 3, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 5]);
        truncated.putBytes(0, encoded, 0, encodedLength - 5);

        assertThrows(IllegalArgumentException.class,
                () -> AeronSnapshotRequestCodec.decode(truncated, 0, encodedLength - 5));
    }

    @Test
    void rejectsChannelLengthThatWouldAllocatePastTheFrame() {
        UnsafeBuffer buffer = AeronSnapshotRequestCodec.allocateBuffer(1);
        int encodedLength = AeronSnapshotRequestCodec.encode(1L, 2L, "a", 3, buffer);
        buffer.putInt(20, Integer.MAX_VALUE);

        assertThrows(IllegalArgumentException.class,
                () -> AeronSnapshotRequestCodec.decode(buffer, 0, encodedLength));
    }
}
