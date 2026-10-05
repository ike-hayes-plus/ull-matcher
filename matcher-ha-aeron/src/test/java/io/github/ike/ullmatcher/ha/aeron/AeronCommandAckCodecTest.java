package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Wire contract for the direct acknowledgement frame.
 */
final class AeronCommandAckCodecTest {
    @Test
    void encodedLengthIsASingleSequenceNumber() {
        assertEquals(Long.BYTES, AeronCommandAckCodec.ENCODED_LENGTH);
        assertEquals(Long.BYTES, AeronCommandAckCodec.allocateBuffer().capacity());
    }

    @Test
    void roundTripsBoundarySequenceNumbers() {
        for (long sequence : new long[]{0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}) {
            UnsafeBuffer buffer = AeronCommandAckCodec.allocateBuffer();

            assertEquals(AeronCommandAckCodec.ENCODED_LENGTH, AeronCommandAckCodec.encode(sequence, buffer));
            assertEquals(sequence, AeronCommandAckCodec.decode(buffer, 0));
        }
    }

    @Test
    void decodesFromNonZeroOffset() {
        UnsafeBuffer encoded = AeronCommandAckCodec.allocateBuffer();
        AeronCommandAckCodec.encode(1234567890123L, encoded);

        int offset = 16;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + AeronCommandAckCodec.ENCODED_LENGTH]);
        framed.putBytes(offset, encoded, 0, AeronCommandAckCodec.ENCODED_LENGTH);

        assertEquals(1234567890123L, AeronCommandAckCodec.decode(framed, offset));
    }

    @Test
    void rejectsTruncatedFrame() {
        UnsafeBuffer truncated = new UnsafeBuffer(new byte[AeronCommandAckCodec.ENCODED_LENGTH - 1]);

        assertThrows(IndexOutOfBoundsException.class, () -> AeronCommandAckCodec.decode(truncated, 0));
    }
}
