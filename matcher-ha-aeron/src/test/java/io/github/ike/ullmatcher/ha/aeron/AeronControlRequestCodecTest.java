package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the control-plane request frame.
 */
final class AeronControlRequestCodecTest {
    @Test
    void roundTripsEveryField() {
        String responseChannel = "aeron:udp?endpoint=127.0.0.1:20601";
        int channelLength = responseChannel.getBytes(StandardCharsets.UTF_8).length;

        UnsafeBuffer buffer = AeronControlRequestCodec.allocateBuffer(channelLength);
        int encodedLength = AeronControlRequestCodec.encode(
                0x0102030405060708L, 0x1122334455667788L, responseChannel, 31337, buffer);

        assertEquals(AeronControlRequestCodec.encodedLength(channelLength), encodedLength);
        assertEquals(buffer.capacity(), encodedLength);

        AeronControlRequestCodec.Request decoded = AeronControlRequestCodec.decode(buffer, 0, encodedLength);

        assertEquals(new AeronControlRequestCodec.Request(
                0x0102030405060708L, 0x1122334455667788L, responseChannel, 31337), decoded);
    }

    @Test
    void encodedLengthIsTheFixedHeaderPlusChannelBytes() {
        assertEquals(Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES,
                AeronControlRequestCodec.encodedLength(0));
        assertEquals(AeronControlRequestCodec.encodedLength(0) + 12,
                AeronControlRequestCodec.encodedLength(12));
    }

    @Test
    void roundTripsEmptyChannelAndExtremeIdentifiers() {
        UnsafeBuffer buffer = AeronControlRequestCodec.allocateBuffer(0);
        int encodedLength = AeronControlRequestCodec.encode(
                Long.MIN_VALUE, Long.MAX_VALUE, "", Integer.MIN_VALUE, buffer);

        AeronControlRequestCodec.Request decoded = AeronControlRequestCodec.decode(buffer, 0, encodedLength);

        assertEquals(Long.MIN_VALUE, decoded.requestId());
        assertEquals(Long.MAX_VALUE, decoded.sessionId());
        assertEquals("", decoded.responseChannel());
        assertEquals(Integer.MIN_VALUE, decoded.responseStreamId());
    }

    @Test
    void roundTripsMultiByteChannel() {
        String responseChannel = "aeron:ipc?别名=控制面";
        int channelLength = responseChannel.getBytes(StandardCharsets.UTF_8).length;

        UnsafeBuffer buffer = AeronControlRequestCodec.allocateBuffer(channelLength);
        int encodedLength = AeronControlRequestCodec.encode(1L, 2L, responseChannel, 3, buffer);

        assertEquals(responseChannel,
                AeronControlRequestCodec.decode(buffer, 0, encodedLength).responseChannel());
    }

    @Test
    void decodesFromNonZeroOffset() {
        String responseChannel = "aeron:ipc";
        UnsafeBuffer encoded = AeronControlRequestCodec.allocateBuffer(responseChannel.length());
        int encodedLength = AeronControlRequestCodec.encode(5L, 6L, responseChannel, 7, encoded);

        int offset = 24;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        assertEquals(new AeronControlRequestCodec.Request(5L, 6L, responseChannel, 7),
                AeronControlRequestCodec.decode(framed, offset, encodedLength));
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        UnsafeBuffer tooSmall = AeronControlRequestCodec.allocateBuffer(0);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronControlRequestCodec.encode(1L, 2L, "aeron:ipc", 3, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsTruncatedFrame() {
        String responseChannel = "aeron:ipc";
        UnsafeBuffer encoded = AeronControlRequestCodec.allocateBuffer(responseChannel.length());
        int encodedLength = AeronControlRequestCodec.encode(1L, 2L, responseChannel, 3, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 4]);
        truncated.putBytes(0, encoded, 0, encodedLength - 4);

        assertThrows(IllegalArgumentException.class,
                () -> AeronControlRequestCodec.decode(truncated, 0, encodedLength - 4));
    }

    @Test
    void rejectsChannelLengthThatWouldAllocatePastTheFrame() {
        UnsafeBuffer buffer = AeronControlRequestCodec.allocateBuffer(1);
        int encodedLength = AeronControlRequestCodec.encode(1L, 2L, "a", 3, buffer);
        buffer.putInt(20, -1);

        assertThrows(IllegalArgumentException.class,
                () -> AeronControlRequestCodec.decode(buffer, 0, encodedLength));
    }
}
