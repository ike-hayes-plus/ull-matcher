package io.github.ike.ullmatcher.sdk;

public record CancelOrderRequest(long orderId, String idempotencyKey) {
    public CancelOrderRequest {
        if (orderId <= 0L) {
            throw new IllegalArgumentException("orderId must be positive");
        }
    }
}
