package io.github.ike.ullmatcher.server.security;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class IngressAuthConfigTest {
    @Test
    void disabledConfigAcceptsEverything() {
        IngressAuthConfig config = IngressAuthConfig.disabled();

        assertFalse(config.enabled());
        assertTrue(config.matchesBinaryHandshake(null));
        assertEquals(IngressAuthConfig.DEFAULT_API_KEY_HEADER, config.apiKeyHeader());
    }

    @Test
    void parsesCommaSeparatedKeysAndTrimsWhitespace() {
        IngressAuthConfig config = IngressAuthConfig.fromCommaSeparated(" key-a , key-b ,, ", "X-Custom");

        assertTrue(config.enabled());
        assertEquals(Set.of("key-a", "key-b"), config.apiKeys());
        assertEquals("X-Custom", config.apiKeyHeader());
    }

    @Test
    void blankInputsFallBackToDisabledAndDefaultHeader() {
        assertFalse(IngressAuthConfig.fromCommaSeparated(null, null).enabled());
        assertFalse(IngressAuthConfig.fromCommaSeparated("", null).enabled());
        assertFalse(IngressAuthConfig.fromCommaSeparated("  ", null).enabled());
        assertFalse(IngressAuthConfig.fromCommaSeparated(" , , ", null).enabled());
        assertEquals(IngressAuthConfig.DEFAULT_API_KEY_HEADER,
                IngressAuthConfig.fromCommaSeparated("key-a", "  ").apiKeyHeader());
    }

    @Test
    void rejectsBlankHeaderAndOversizedKeys() {
        assertThrows(IllegalArgumentException.class, () -> new IngressAuthConfig(Set.of("key"), " "));
        assertThrows(IllegalArgumentException.class,
                () -> new IngressAuthConfig(Set.of("x".repeat(IngressAuthConfig.BINARY_HANDSHAKE_BYTES + 1)), "H"));
        assertThrows(IllegalArgumentException.class, () -> new IngressAuthConfig(Set.of(" "), "H"));
        assertThrows(NullPointerException.class, () -> new IngressAuthConfig(null, "H"));
        assertThrows(NullPointerException.class, () -> new IngressAuthConfig(Set.of("key"), null));
    }

    @Test
    void binaryHandshakeMatchesZeroPaddedKey() {
        IngressAuthConfig config = IngressAuthConfig.fromCommaSeparated("key-a,key-b", null);

        assertTrue(config.matchesBinaryHandshake(handshake("key-a")));
        assertTrue(config.matchesBinaryHandshake(handshake("key-b")));
        assertFalse(config.matchesBinaryHandshake(handshake("key-c")));
    }

    @Test
    void binaryHandshakeRejectsNullAndWronglySizedBuffers() {
        IngressAuthConfig config = IngressAuthConfig.fromCommaSeparated("key-a", null);

        assertFalse(config.matchesBinaryHandshake(null));
        assertFalse(config.matchesBinaryHandshake(ByteBuffer.allocate(16)));
        assertFalse(config.matchesBinaryHandshake(ByteBuffer.allocate(64)));
    }

    @Test
    void handshakePaddingIsFixedWidthAndZeroFilled() {
        byte[] padded = IngressAuthConfig.padHandshakeBytes("abc");

        assertEquals(IngressAuthConfig.BINARY_HANDSHAKE_BYTES, padded.length);
        assertEquals('a', padded[0]);
        assertEquals('c', padded[2]);
        for (int index = 3; index < padded.length; index++) {
            assertEquals(0, padded[index], "byte " + index + " must be zero padding");
        }
    }

    @Test
    void handshakePaddingRejectsNullAndOversizedKeys() {
        assertThrows(NullPointerException.class, () -> IngressAuthConfig.padHandshakeBytes(null));
        assertThrows(IllegalArgumentException.class,
                () -> IngressAuthConfig.padHandshakeBytes("x".repeat(IngressAuthConfig.BINARY_HANDSHAKE_BYTES + 1)));
    }

    @Test
    void httpAuthorizeAcceptsHeaderAndBearerAndRejectsMissingOrWrongKeys() {
        IngressAuthConfig disabled = IngressAuthConfig.disabled();
        assertTrue(disabled.authorize(new HttpServerExchange(null)));

        IngressAuthConfig config = IngressAuthConfig.fromCommaSeparated("secret-key", null);
        assertFalse(config.authorize(new HttpServerExchange(null)));

        HttpServerExchange header = new HttpServerExchange(null);
        header.getRequestHeaders().put(new HttpString(IngressAuthConfig.DEFAULT_API_KEY_HEADER), " secret-key ");
        assertTrue(config.authorize(header));

        HttpServerExchange bearer = new HttpServerExchange(null);
        bearer.getRequestHeaders().put(Headers.AUTHORIZATION, "Bearer secret-key");
        assertTrue(config.authorize(bearer));

        HttpServerExchange emptyBearer = new HttpServerExchange(null);
        emptyBearer.getRequestHeaders().put(Headers.AUTHORIZATION, "Bearer   ");
        assertFalse(config.authorize(emptyBearer));

        HttpServerExchange basic = new HttpServerExchange(null);
        basic.getRequestHeaders().put(Headers.AUTHORIZATION, "Basic abc");
        assertFalse(config.authorize(basic));

        HttpServerExchange wrong = new HttpServerExchange(null);
        wrong.getRequestHeaders().put(new HttpString(IngressAuthConfig.DEFAULT_API_KEY_HEADER), "other");
        assertFalse(config.authorize(wrong));
    }

    @Test
    void maximumLengthKeyIsAccepted() {
        String key = "x".repeat(IngressAuthConfig.BINARY_HANDSHAKE_BYTES);
        IngressAuthConfig config = IngressAuthConfig.fromCommaSeparated(key, null);

        assertTrue(config.matchesBinaryHandshake(handshake(key)));
    }

    private static ByteBuffer handshake(String key) {
        byte[] raw = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(IngressAuthConfig.BINARY_HANDSHAKE_BYTES);
        buffer.put(raw);
        buffer.position(0);
        return buffer;
    }
}
