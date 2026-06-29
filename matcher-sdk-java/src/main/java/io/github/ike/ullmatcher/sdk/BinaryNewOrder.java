package io.github.ike.ullmatcher.sdk;

public record BinaryNewOrder(long userId,
                             long orderId,
                             long price,
                             long quantity,
                             long ttlMillis,
                             byte side,
                             byte orderType,
                             byte timeInForce) {
    public BinaryNewOrder {
        if (userId <= 0L || orderId <= 0L || price < 0L || quantity <= 0L) {
            throw new IllegalArgumentException("userId, orderId, price and quantity are invalid");
        }
    }

    public static BinaryNewOrder buyLimit(long userId, long orderId, long price, long quantity) {
        return new BinaryNewOrder(userId, orderId, price, quantity, -1L, (byte) 'B', (byte) 'L', (byte) 'G');
    }
}
