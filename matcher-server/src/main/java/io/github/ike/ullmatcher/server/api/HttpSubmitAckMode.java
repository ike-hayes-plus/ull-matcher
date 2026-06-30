package io.github.ike.ullmatcher.server.api;

import java.util.Locale;

public enum HttpSubmitAckMode {
    LOCAL,
    COMMITTED;

    public static HttpSubmitAckMode parse(String raw, HttpSubmitAckMode defaultValue) {
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        return switch (normalized) {
            case "LOCAL", "LOCAL_ACCEPTED" -> LOCAL;
            case "COMMITTED", "REPLICATION_COMMITTED", "REPLICATED" -> COMMITTED;
            default -> throw new BadRequestException("invalid ack mode: " + raw);
        };
    }
}
