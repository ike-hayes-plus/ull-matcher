package io.github.ike.ullmatcher.server.engine;

public record OrderStateView(
        long orderId,
        long sequence,
        int symbolId,
        String status,
        String rejectReason,
        long remaining,
        String side,
        String orderType,
        String timeInForce,
        Long price,
        Long quantity,
        Long expireAtEpochMillis,
        long updatedAtEpochMillis
) {
}
