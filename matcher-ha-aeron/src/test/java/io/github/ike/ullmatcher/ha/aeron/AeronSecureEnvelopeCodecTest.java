package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the secure session envelope that wraps every post-handshake message.
 * The session id and replay counter must survive intact, and an envelope with an unknown version
 * must be rejected before the ciphertext is read.
 */
final class AeronSecureEnvelopeCodecTest {
    @Test
    void roundTripsEveryField() {
        byte[] ciphertext = {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};

        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(ciphertext.length);
        int encodedLength = AeronSecureEnvelopeCodec.encode(
                7, 0x1122334455667788L, 0x7FFFFFFFFFFFFFFFL, ciphertext, ciphertext.length, buffer);

        assertEquals(AeronSecureEnvelopeCodec.encodedLength(ciphertext.length), encodedLength);
        assertEquals(encodedLength, buffer.capacity());

        AeronSecureEnvelopeCodec.DecodedEnvelope decoded =
                AeronSecureEnvelopeCodec.decode(buffer, 0, encodedLength);

        assertEquals(7, decoded.messageType());
        assertEquals(0x1122334455667788L, decoded.sessionId());
        assertEquals(0x7FFFFFFFFFFFFFFFL, decoded.counter());
        assertArrayEquals(ciphertext, decoded.ciphertext());
    }

    @Test
    void encodedLengthMatchesTheFixedHeaderPlusCiphertext() {
        assertEquals(Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES,
                AeronSecureEnvelopeCodec.encodedLength(0));
        assertEquals(AeronSecureEnvelopeCodec.encodedLength(0) + 16,
                AeronSecureEnvelopeCodec.encodedLength(16));
    }

    @Test
    void roundTripsEmptyCiphertext() {
        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(0);
        int encodedLength = AeronSecureEnvelopeCodec.encode(0, 0L, 0L, new byte[0], 0, buffer);

        AeronSecureEnvelopeCodec.DecodedEnvelope decoded =
                AeronSecureEnvelopeCodec.decode(buffer, 0, encodedLength);

        assertEquals(0, decoded.messageType());
        assertArrayEquals(new byte[0], decoded.ciphertext());
    }

    @Test
    void encodesOnlyThePrefixRequestedByCiphertextLength() {
        byte[] ciphertext = {1, 2, 3, 4, 5, 6};

        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(ciphertext.length);
        int encodedLength = AeronSecureEnvelopeCodec.encode(1, 2L, 3L, ciphertext, 2, buffer);

        assertEquals(AeronSecureEnvelopeCodec.encodedLength(2), encodedLength);
        assertArrayEquals(new byte[]{1, 2},
                AeronSecureEnvelopeCodec.decode(buffer, 0, encodedLength).ciphertext());
    }

    @Test
    void roundTripsExtremeNumericValues() {
        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(1);
        int encodedLength = AeronSecureEnvelopeCodec.encode(
                Integer.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, new byte[]{-1}, 1, buffer);

        AeronSecureEnvelopeCodec.DecodedEnvelope decoded =
                AeronSecureEnvelopeCodec.decode(buffer, 0, encodedLength);

        assertEquals(Integer.MIN_VALUE, decoded.messageType());
        assertEquals(Long.MIN_VALUE, decoded.sessionId());
        assertEquals(Long.MIN_VALUE, decoded.counter());
    }

    @Test
    void decodesFromNonZeroOffset() {
        byte[] ciphertext = {5, 6, 7};
        UnsafeBuffer encoded = AeronSecureEnvelopeCodec.allocateBuffer(ciphertext.length);
        int encodedLength = AeronSecureEnvelopeCodec.encode(
                3, 44L, 55L, ciphertext, ciphertext.length, encoded);

        int offset = 12;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        AeronSecureEnvelopeCodec.DecodedEnvelope decoded =
                AeronSecureEnvelopeCodec.decode(framed, offset, encodedLength);

        assertEquals(3, decoded.messageType());
        assertEquals(44L, decoded.sessionId());
        assertEquals(55L, decoded.counter());
        assertArrayEquals(ciphertext, decoded.ciphertext());
    }

    @Test
    void rejectsNegativeCiphertextLength() {
        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(8);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureEnvelopeCodec.encode(1, 1L, 1L, new byte[8], -1, buffer));

        assertTrue(failure.getMessage().contains("invalid ciphertextLength"));
    }

    @Test
    void rejectsCiphertextLengthBeyondTheSourceArray() {
        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(16);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureEnvelopeCodec.encode(1, 1L, 1L, new byte[4], 5, buffer));

        assertTrue(failure.getMessage().contains("invalid ciphertextLength"));
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        UnsafeBuffer tooSmall = new UnsafeBuffer(new byte[AeronSecureEnvelopeCodec.encodedLength(4) - 1]);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureEnvelopeCodec.encode(1, 1L, 1L, new byte[4], 4, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsUnsupportedVersion() {
        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(2);
        int encodedLength = AeronSecureEnvelopeCodec.encode(1, 1L, 1L, new byte[]{1, 2}, 2, buffer);
        buffer.putInt(0, AeronSecureEnvelopeCodec.VERSION + 7);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureEnvelopeCodec.decode(buffer, 0, encodedLength));

        assertTrue(failure.getMessage().contains("unsupported secure envelope version"));
    }

    @Test
    void rejectsTruncatedFrame() {
        UnsafeBuffer encoded = AeronSecureEnvelopeCodec.allocateBuffer(8);
        int encodedLength = AeronSecureEnvelopeCodec.encode(1, 1L, 1L, new byte[8], 8, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 5]);
        truncated.putBytes(0, encoded, 0, encodedLength - 5);

        assertThrows(IllegalArgumentException.class,
                () -> AeronSecureEnvelopeCodec.decode(truncated, 0, encodedLength - 5));
    }

    @Test
    void rejectsCiphertextLengthThatWouldAllocatePastTheFrame() {
        UnsafeBuffer buffer = AeronSecureEnvelopeCodec.allocateBuffer(1);
        int encodedLength = AeronSecureEnvelopeCodec.encode(1, 1L, 1L, new byte[]{1}, 1, buffer);
        buffer.putInt(24, Integer.MAX_VALUE);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureEnvelopeCodec.decode(buffer, 0, encodedLength));

        assertTrue(failure.getMessage().contains("exceeds frame"));
    }
}
