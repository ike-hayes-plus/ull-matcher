package io.github.ike.ullmatcher.server.api;

import io.undertow.server.HttpServerExchange;

final class HttpSubmitRequestPolicy {
    private HttpSubmitRequestPolicy() {}

    static HttpSubmitAckMode resolveAckMode(
            HttpServerExchange exchange,
            String bodyAckMode,
            HttpSubmitAckMode defaultSubmitAckMode
    ) {
        String queryAck = exchange.getQueryParameters().containsKey("ack")
                ? exchange.getQueryParameters().get("ack").getFirst()
                : null;
        String headerAck = exchange.getRequestHeaders().getFirst("X-Ull-Ack");
        if (queryAck != null && !queryAck.isBlank()) {
            return HttpSubmitAckMode.parse(queryAck, defaultSubmitAckMode);
        }
        if (headerAck != null && !headerAck.isBlank()) {
            return HttpSubmitAckMode.parse(headerAck, defaultSubmitAckMode);
        }
        return HttpSubmitAckMode.parse(bodyAckMode, defaultSubmitAckMode);
    }

    static String resolveIdempotencyKey(HttpServerExchange exchange, String bodyKey, String fallbackKey) {
        String headerKey = exchange.getRequestHeaders().getFirst("Idempotency-Key");
        if (headerKey != null && !headerKey.isBlank()) {
            return headerKey.trim();
        }
        if (bodyKey != null && !bodyKey.isBlank()) {
            return bodyKey.trim();
        }
        return fallbackKey;
    }

    static String defaultOrderIdempotencyKey(long userId, long orderId) {
        return "new-order:" + userId + ":" + orderId;
    }

    static String defaultCancelIdempotencyKey(long orderId) {
        return "cancel-order:" + orderId;
    }
}
