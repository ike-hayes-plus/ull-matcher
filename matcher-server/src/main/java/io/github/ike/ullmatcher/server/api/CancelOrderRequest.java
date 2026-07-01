package io.github.ike.ullmatcher.server.api;

record CancelOrderRequest(long orderId, String idempotencyKey, String ack) {
}
