package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire contract for the secure handshake response frame. An accepted response carries the server
 * credentials; a rejected one is truncated right after the error message, so the two shapes have
 * different encoded lengths and must both decode into a complete record.
 */
final class AeronSecureHandshakeResponseCodecTest {
    @Test
    void roundTripsAcceptedResponseWithCertificateChain() {
        AeronSecureHandshakeResponseCodec.Response response = new AeronSecureHandshakeResponseCodec.Response(
                0x1122334455667788L,
                -7L,
                true,
                "node-standby",
                1_700_000_000_000L,
                1_700_000_030_000L,
                "",
                new byte[]{9, 8, 7, 6},
                new byte[]{(byte) 0xFF, 0x01},
                List.of(new byte[]{1, 2, 3}, new byte[]{4}),
                new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}
        );

        UnsafeBuffer buffer = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
        int encodedLength = AeronSecureHandshakeResponseCodec.encode(response, buffer);

        assertEquals(AeronSecureHandshakeResponseCodec.encodedLength(response), encodedLength);
        assertEquals(encodedLength, buffer.capacity());

        AeronSecureHandshakeResponseCodec.Response decoded =
                AeronSecureHandshakeResponseCodec.decode(buffer, 0, encodedLength);

        assertTrue(decoded.accepted());
        assertResponseEquals(response, decoded);
    }

    @Test
    void roundTripsAcceptedResponseWithoutCertificates() {
        AeronSecureHandshakeResponseCodec.Response response = new AeronSecureHandshakeResponseCodec.Response(
                1L, 2L, true, "node-b", 3L, 4L, "", new byte[0], new byte[0], List.of(), new byte[0]);

        UnsafeBuffer buffer = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
        int encodedLength = AeronSecureHandshakeResponseCodec.encode(response, buffer);

        AeronSecureHandshakeResponseCodec.Response decoded =
                AeronSecureHandshakeResponseCodec.decode(buffer, 0, encodedLength);

        assertResponseEquals(response, decoded);
        assertTrue(decoded.certificateChainDer().isEmpty());
    }

    @Test
    void rejectedResponseKeepsErrorMessageAndClearsCredentials() {
        AeronSecureHandshakeResponseCodec.Response response = new AeronSecureHandshakeResponseCodec.Response(
                55L,
                66L,
                false,
                "ignored-node",
                10L,
                20L,
                "certificate chain not trusted",
                new byte[]{1, 2, 3},
                new byte[]{4, 5},
                List.of(new byte[]{6}),
                new byte[]{7}
        );

        UnsafeBuffer buffer = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
        int encodedLength = AeronSecureHandshakeResponseCodec.encode(response, buffer);

        assertEquals(AeronSecureHandshakeResponseCodec.encodedLength(response), encodedLength);

        AeronSecureHandshakeResponseCodec.Response decoded =
                AeronSecureHandshakeResponseCodec.decode(buffer, 0, encodedLength);

        assertEquals(55L, decoded.requestId());
        assertEquals(66L, decoded.sessionId());
        assertFalse(decoded.accepted());
        assertEquals(10L, decoded.createdAtMillis());
        assertEquals(20L, decoded.expiresAtMillis());
        assertEquals("certificate chain not trusted", decoded.errorMessage());
        assertEquals("", decoded.nodeId());
        assertArrayEquals(new byte[0], decoded.serverNonce());
        assertArrayEquals(new byte[0], decoded.serverEphemeralPublicKey());
        assertTrue(decoded.certificateChainDer().isEmpty());
        assertArrayEquals(new byte[0], decoded.signature());
    }

    @Test
    void rejectedResponseEncodingStopsAfterErrorMessage() {
        AeronSecureHandshakeResponseCodec.Response rejected = new AeronSecureHandshakeResponseCodec.Response(
                1L, 2L, false, "node-c", 3L, 4L, "nope",
                new byte[64], new byte[64], List.of(new byte[64]), new byte[64]);
        AeronSecureHandshakeResponseCodec.Response accepted = new AeronSecureHandshakeResponseCodec.Response(
                1L, 2L, true, "node-c", 3L, 4L, "nope",
                new byte[64], new byte[64], List.of(new byte[64]), new byte[64]);

        int header = 4 + 8 + 8 + 4 + 8 + 8;
        assertEquals(header + 4 + "nope".length(), AeronSecureHandshakeResponseCodec.encodedLength(rejected));
        assertTrue(AeronSecureHandshakeResponseCodec.encodedLength(accepted)
                > AeronSecureHandshakeResponseCodec.encodedLength(rejected));
    }

    @Test
    void roundTripsExtremeNumericValues() {
        AeronSecureHandshakeResponseCodec.Response response = new AeronSecureHandshakeResponseCodec.Response(
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                true,
                "节点-\uD83D\uDE80",
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                "",
                new byte[]{-1, -128, 127},
                new byte[]{0},
                List.of(new byte[0], new byte[]{-1}),
                new byte[]{-128}
        );

        UnsafeBuffer buffer = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
        int encodedLength = AeronSecureHandshakeResponseCodec.encode(response, buffer);

        assertResponseEquals(response, AeronSecureHandshakeResponseCodec.decode(buffer, 0, encodedLength));
    }

    @Test
    void decodesFromNonZeroOffset() {
        AeronSecureHandshakeResponseCodec.Response response = sampleAccepted();
        UnsafeBuffer encoded = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
        int encodedLength = AeronSecureHandshakeResponseCodec.encode(response, encoded);

        int offset = 16;
        UnsafeBuffer framed = new UnsafeBuffer(new byte[offset + encodedLength]);
        framed.putBytes(offset, encoded, 0, encodedLength);

        assertResponseEquals(response, AeronSecureHandshakeResponseCodec.decode(framed, offset, encodedLength));
    }

    @Test
    void rejectsBufferSmallerThanEncodedLength() {
        AeronSecureHandshakeResponseCodec.Response response = sampleAccepted();
        UnsafeBuffer tooSmall = new UnsafeBuffer(
                new byte[AeronSecureHandshakeResponseCodec.encodedLength(response) - 1]);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeResponseCodec.encode(response, tooSmall));

        assertTrue(failure.getMessage().contains("buffer capacity too small"));
    }

    @Test
    void rejectsUnsupportedVersion() {
        AeronSecureHandshakeResponseCodec.Response response = sampleAccepted();
        UnsafeBuffer buffer = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
        int encodedLength = AeronSecureHandshakeResponseCodec.encode(response, buffer);
        buffer.putInt(0, 0);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeResponseCodec.decode(buffer, 0, encodedLength));

        assertTrue(failure.getMessage().contains("unsupported secure handshake response version"));
    }

    @Test
    void rejectsTruncatedFrame() {
        AeronSecureHandshakeResponseCodec.Response response = sampleAccepted();
        UnsafeBuffer encoded = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
        int encodedLength = AeronSecureHandshakeResponseCodec.encode(response, encoded);

        UnsafeBuffer truncated = new UnsafeBuffer(new byte[encodedLength - 6]);
        truncated.putBytes(0, encoded, 0, encodedLength - 6);

        assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeResponseCodec.decode(truncated, 0, encodedLength - 6));
    }

    @Test
    void rejectsFieldLengthThatWouldAllocatePastTheFrame() {
        int frameLength = 44;
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[frameLength]);
        buffer.putInt(0, AeronSecureHandshakeResponseCodec.VERSION);
        buffer.putInt(40, Integer.MAX_VALUE);

        IllegalArgumentException huge = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeResponseCodec.decode(buffer, 0, frameLength));
        assertTrue(huge.getMessage().contains("exceeds frame"));

        buffer.putInt(40, -1);
        assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeResponseCodec.decode(buffer, 0, frameLength));
    }

    @Test
    void rejectsCertificateCountThatWouldAllocatePastTheFrame() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[60]);
        buffer.putInt(0, AeronSecureHandshakeResponseCodec.VERSION);
        buffer.putInt(20, 1);
        buffer.putInt(56, Integer.MAX_VALUE);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> AeronSecureHandshakeResponseCodec.decode(buffer, 0, buffer.capacity()));

        assertTrue(failure.getMessage().contains("certificate count"));
    }

    private static AeronSecureHandshakeResponseCodec.Response sampleAccepted() {
        return new AeronSecureHandshakeResponseCodec.Response(
                101L, 202L, true, "node-d", 303L, 404L, "",
                new byte[]{1, 2}, new byte[]{3, 4, 5}, List.of(new byte[]{6, 7}), new byte[]{8});
    }

    private static void assertResponseEquals(AeronSecureHandshakeResponseCodec.Response expected,
                                             AeronSecureHandshakeResponseCodec.Response actual) {
        assertEquals(expected.requestId(), actual.requestId());
        assertEquals(expected.sessionId(), actual.sessionId());
        assertEquals(expected.accepted(), actual.accepted());
        assertEquals(expected.nodeId(), actual.nodeId());
        assertEquals(expected.createdAtMillis(), actual.createdAtMillis());
        assertEquals(expected.expiresAtMillis(), actual.expiresAtMillis());
        assertEquals(expected.errorMessage(), actual.errorMessage());
        assertArrayEquals(expected.serverNonce(), actual.serverNonce());
        assertArrayEquals(expected.serverEphemeralPublicKey(), actual.serverEphemeralPublicKey());
        assertEquals(expected.certificateChainDer().size(), actual.certificateChainDer().size());
        for (int i = 0; i < expected.certificateChainDer().size(); i++) {
            assertArrayEquals(expected.certificateChainDer().get(i), actual.certificateChainDer().get(i));
        }
        assertArrayEquals(expected.signature(), actual.signature());
    }
}
