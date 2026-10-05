package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the secure handshake request frame. Every field must survive a round trip, and
 * a frame carrying an unknown version must be rejected rather than decoded into garbage.
 */
final class AeronSecureHandshakeRequestCodecTest {
    @Test
    void roundTripsEveryFieldWithCertificateChain() {
        AeronSecureHandshakeRequestCodec.Request request = new AeronSecureHandshakeRequestCodec.Request(
                0x0102030405060708L,
                -42L,
                "node-primary",
                "aeron:udp?endpoint=127.0.0.1:20201",
                9876,
                1_700_000_000_123L,
                1_700_000_060_456L,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{(byte) 0x80, 0x00, 0x7F},
                List.of(new byte[]{10, 11}, new byte[]{20, 21, 22}, new byte[]{30}),
                new byte[]{(byte) 0xAA, (byte) 0xBB}
        );

        UnsafeBuffer buffer = AeronSecureHandshakeRequestCodec.allocateBuffer(request);
        int encodedLength = AeronSecureHandshakeRequestCodec.encode(request, buffer);

        assertEquals(AeronSecureHandshakeRequestCodec.encodedLength(request), encodedLength);
        assertEquals(encodedLength, buffer.capacity());

        AeronSecureHandshakeRequestCodec.Request decoded =
                AeronSecureHandshakeRequestCodec.decode(buffer, 0, encodedLength);

        assertRequestEquals(request, decoded);
    }

    @Test
    void roundTripsEmptyCertificateChainAndEmptyByteFields() {
        AeronSecureHandshakeRequestCodec.Request request = new AeronSecureHandshakeRequestCodec.Request(
                0L,
                0L,
                "",
                "",
                0,
                0L,
                0L,
                new byte[0],
                new byte[0],
                List.of(),
                new byte[0]
        );

        UnsafeBuffer buffer = AeronSecureHandshakeRequestCodec.allocateBuffer(request);
        int encodedLength = AeronSecureHandshakeRequestCodec.encode(request, buffer);

        AeronSecureHandshakeRequestCodec.Request decoded =
                AeronSecureHandshakeRequestCodec.decode(buffer, 0, encodedLength);

        assertRequestEquals(request, decoded);
        assertTrue(decoded.certificateChainDer().isEmpty());
    }

    @Test
    void roundTripsExtremeNumericValues() {
        AeronSecureHandshakeRequestCodec.Request request = new AeronSecureHandshakeRequestCodec.Request(
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                "节点-\uD83D\uDE80",
                "aeron:ipc?term-length=64k",
                Integer.MIN_VALUE,
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                new byte[]{-1, -128, 127},
                new byte[]{0},
                List.of(new byte[0]),
                new byte[]{-1}
        );

        UnsafeBuffer buffer = AeronSecureHandshakeRequestCodec.allocateBuffer(request);
        int encodedLength = AeronSecureHandshakeRequestCodec.encode(request, buffer);

        assertRequestEquals(request, AeronSecureHandshakeRequestCodec.decode(buffer, 0, encodedLength));
    }

    @Test
    void decodesFromNonZeroOffset() {
        AeronSecureHandshakeRequestCodec.Request request = sampleRequest();
        UnsafeBuffer encoded = AeronSecureHandshakeRequestCodec.allocateBuffer(request);
        int encodedLength = AeronSecureHandshakeRequestCodec.encode(request, encoded);

        int offset = 24;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        assertRequestEquals(request, AeronSecureHandshakeRequestCodec.decode(framed, offset, encodedLength));
    }

    @Test
    void encodedLengthAccountsForUtf8ExpansionOfTextFields() {
        AeronSecureHandshakeRequestCodec.Request request = new AeronSecureHandshakeRequestCodec.Request(
                1L, 2L, "节点", "通道", 3, 4L, 5L,
                new byte[2], new byte[3], List.of(), new byte[1]);

        int expected = 4 + 8 + 8 + 4 + 8 + 8
                + 4 + "节点".getBytes(StandardCharsets.UTF_8).length
                + 4 + "通道".getBytes(StandardCharsets.UTF_8).length
                + 4 + 2
                + 4 + 3
                + 4
                + 4 + 1;

        assertEquals(expected, AeronSecureHandshakeRequestCodec.encodedLength(request));
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        AeronSecureHandshakeRequestCodec.Request request = sampleRequest();
        UnsafeBuffer tooSmall = new UnsafeBuffer(
                new byte[AeronSecureHandshakeRequestCodec.encodedLength(request) - 1]);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeRequestCodec.encode(request, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsUnsupportedVersion() {
        AeronSecureHandshakeRequestCodec.Request request = sampleRequest();
        UnsafeBuffer buffer = AeronSecureHandshakeRequestCodec.allocateBuffer(request);
        int encodedLength = AeronSecureHandshakeRequestCodec.encode(request, buffer);
        buffer.putInt(0, AeronSecureHandshakeRequestCodec.VERSION + 1);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeRequestCodec.decode(buffer, 0, encodedLength));

        assertTrue(failure.getMessage().contains("unsupported secure handshake request version"));
    }

    @Test
    void rejectsTruncatedFrame() {
        AeronSecureHandshakeRequestCodec.Request request = sampleRequest();
        UnsafeBuffer encoded = AeronSecureHandshakeRequestCodec.allocateBuffer(request);
        int encodedLength = AeronSecureHandshakeRequestCodec.encode(request, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 4]);
        truncated.putBytes(0, encoded, 0, encodedLength - 4);

        assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeRequestCodec.decode(truncated, 0, encodedLength - 4));
    }

    @Test
    void rejectsFieldLengthThatWouldAllocatePastTheFrame() {
        int frameLength = 44;
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[frameLength]);
        buffer.putInt(0, AeronSecureHandshakeRequestCodec.VERSION);
        buffer.putInt(40, Integer.MAX_VALUE);

        IllegalArgumentException huge = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeRequestCodec.decode(buffer, 0, frameLength));
        assertTrue(huge.getMessage().contains("exceeds frame"));

        buffer.putInt(40, -1);
        IllegalArgumentException negative = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeRequestCodec.decode(buffer, 0, frameLength));
        assertTrue(negative.getMessage().contains("exceeds frame"));
    }

    @Test
    void rejectsCertificateCountThatWouldAllocatePastTheFrame() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[60]);
        buffer.putInt(0, AeronSecureHandshakeRequestCodec.VERSION);
        buffer.putInt(56, Integer.MAX_VALUE);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeRequestCodec.decode(buffer, 0, buffer.capacity()));

        assertTrue(failure.getMessage().contains("certificate count"));
    }

    private static AeronSecureHandshakeRequestCodec.Request sampleRequest() {
        return new AeronSecureHandshakeRequestCodec.Request(
                7L,
                11L,
                "node-a",
                "aeron:udp?endpoint=127.0.0.1:20301",
                4242,
                100L,
                200L,
                new byte[]{1, 2},
                new byte[]{3, 4, 5},
                List.of(new byte[]{6}),
                new byte[]{7, 8}
        );
    }

    private static void assertRequestEquals(AeronSecureHandshakeRequestCodec.Request expected,
                                            AeronSecureHandshakeRequestCodec.Request actual) {
        assertEquals(expected.requestId(), actual.requestId());
        assertEquals(expected.sessionId(), actual.sessionId());
        assertEquals(expected.nodeId(), actual.nodeId());
        assertEquals(expected.responseChannel(), actual.responseChannel());
        assertEquals(expected.responseStreamId(), actual.responseStreamId());
        assertEquals(expected.createdAtMillis(), actual.createdAtMillis());
        assertEquals(expected.expiresAtMillis(), actual.expiresAtMillis());
        assertArrayEquals(expected.clientNonce(), actual.clientNonce());
        assertArrayEquals(expected.clientEphemeralPublicKey(), actual.clientEphemeralPublicKey());
        assertEquals(expected.certificateChainDer().size(), actual.certificateChainDer().size());
        for (int i = 0; i < expected.certificateChainDer().size(); i++) {
            assertArrayEquals(expected.certificateChainDer().get(i), actual.certificateChainDer().get(i));
        }
        assertArrayEquals(expected.signature(), actual.signature());
    }
}
