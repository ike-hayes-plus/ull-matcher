package io.github.ike.ullmatcher.server.security;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Shared-secret authentication for HTTP and binary ingress.
 */
public record IngressAuthConfig(Set<String> apiKeys, String apiKeyHeader) {
    public static final int BINARY_HANDSHAKE_BYTES = 32;
    public static final String DEFAULT_API_KEY_HEADER = "X-Ull-Api-Key";

    public IngressAuthConfig {
        Objects.requireNonNull(apiKeys, "apiKeys");
        Objects.requireNonNull(apiKeyHeader, "apiKeyHeader");
        if (apiKeyHeader.isBlank()) {
            throw new IllegalArgumentException("apiKeyHeader must not be blank");
        }
        for (String key : apiKeys) {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("ingress api key must not be blank");
            }
            if (key.getBytes(StandardCharsets.UTF_8).length > BINARY_HANDSHAKE_BYTES) {
                throw new IllegalArgumentException("ingress api key exceeds " + BINARY_HANDSHAKE_BYTES
                        + " bytes and cannot be used for the binary handshake");
            }
        }
        apiKeys = Set.copyOf(apiKeys);
    }

    public static IngressAuthConfig disabled() {
        return new IngressAuthConfig(Set.of(), DEFAULT_API_KEY_HEADER);
    }

    public static IngressAuthConfig fromCommaSeparated(String raw, String headerName) {
        String header = headerName == null || headerName.isBlank() ? DEFAULT_API_KEY_HEADER : headerName.trim();
        if (raw == null || raw.isBlank()) {
            return disabled();
        }
        LinkedHashSet<String> keys = new LinkedHashSet<>();
        for (String token : raw.split(",")) {
            String key = token.trim();
            if (!key.isEmpty()) {
                keys.add(key);
            }
        }
        if (keys.isEmpty()) {
            return disabled();
        }
        return new IngressAuthConfig(keys, header);
    }

    public boolean enabled() {
        return !apiKeys.isEmpty();
    }

    public boolean authorize(HttpServerExchange exchange) {
        if (!enabled()) {
            return true;
        }
        String presented = extractCredential(exchange);
        if (presented == null) {
            return false;
        }
        byte[] presentedBytes = presented.getBytes(StandardCharsets.UTF_8);
        boolean matched = false;
        for (String key : apiKeys) {
            matched |= constantTimeEquals(key.getBytes(StandardCharsets.UTF_8), presentedBytes);
        }
        return matched;
    }

    public boolean matchesBinaryHandshake(ByteBuffer buffer) {
        if (!enabled()) {
            return true;
        }
        if (buffer == null || buffer.remaining() != BINARY_HANDSHAKE_BYTES) {
            return false;
        }
        byte[] presented = new byte[BINARY_HANDSHAKE_BYTES];
        buffer.get(presented);
        boolean matched = false;
        for (String key : apiKeys) {
            matched |= constantTimeEquals(padHandshakeBytes(key), presented);
        }
        return matched;
    }

    public static byte[] padHandshakeBytes(String apiKey) {
        Objects.requireNonNull(apiKey, "apiKey");
        byte[] raw = apiKey.getBytes(StandardCharsets.UTF_8);
        if (raw.length > BINARY_HANDSHAKE_BYTES) {
            throw new IllegalArgumentException("ingress api key exceeds " + BINARY_HANDSHAKE_BYTES + " bytes");
        }
        byte[] padded = new byte[BINARY_HANDSHAKE_BYTES];
        System.arraycopy(raw, 0, padded, 0, raw.length);
        return padded;
    }

    private String extractCredential(HttpServerExchange exchange) {
        String apiKey = exchange.getRequestHeaders().getFirst(apiKeyHeader);
        if (apiKey != null && !apiKey.isBlank()) {
            return apiKey.trim();
        }
        String authorization = exchange.getRequestHeaders().getFirst(Headers.AUTHORIZATION);
        if (authorization == null || authorization.isBlank()) {
            return null;
        }
        String trimmed = authorization.trim();
        if (trimmed.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String bearer = trimmed.substring(7).trim();
            return bearer.isEmpty() ? null : bearer;
        }
        return null;
    }

    private static boolean constantTimeEquals(byte[] left, byte[] right) {
        int diff = left.length ^ right.length;
        for (int i = 0; i < BINARY_HANDSHAKE_BYTES; i++) {
            int l = i < left.length ? left[i] : 0;
            int r = i < right.length ? right[i] : 0;
            diff |= l ^ r;
        }
        if (left.length > BINARY_HANDSHAKE_BYTES || right.length > BINARY_HANDSHAKE_BYTES) {
            diff |= 1;
        }
        return diff == 0;
    }
}
