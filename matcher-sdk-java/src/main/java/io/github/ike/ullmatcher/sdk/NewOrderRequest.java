package io.github.ike.ullmatcher.sdk;

public record NewOrderRequest(long userId,
                              long orderId,
                              String side,
                              String orderType,
                              String timeInForce,
                              long price,
                              long quantity,
                              Long ttlMillis,
                              String idempotencyKey) {
    public NewOrderRequest {
        if (userId <= 0L || orderId <= 0L || price < 0L || quantity <= 0L) {
            throw new IllegalArgumentException("userId, orderId, price and quantity are invalid");
        }
        if (side == null || side.isBlank() || orderType == null || orderType.isBlank()
                || timeInForce == null || timeInForce.isBlank()) {
            throw new IllegalArgumentException("side, orderType and timeInForce must not be blank");
        }
    }

    public static NewOrderRequest limit(long userId, long orderId, String side, String timeInForce,
                                        long price, long quantity, String idempotencyKey) {
        return new NewOrderRequest(userId, orderId, side, "LIMIT", timeInForce, price, quantity, null, idempotencyKey);
    }
}
