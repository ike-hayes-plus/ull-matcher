package io.github.ike.ullmatcher.server.engine;

public record TtlAuditEntry(
        long observedAtEpochMillis,
        long orderId,
        String action,
        String source,
        String detail,
        long expireAtEpochMillis
) {
}
