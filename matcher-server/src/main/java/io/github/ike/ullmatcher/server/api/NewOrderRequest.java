package io.github.ike.ullmatcher.server.api;

record NewOrderRequest(long userId,
                       long orderId,
                       String side,
                       String orderType,
                       String timeInForce,
                       long price,
                       long quantity,
                       Long ttlMillis,
                       String idempotencyKey,
                       String ack) {
}
