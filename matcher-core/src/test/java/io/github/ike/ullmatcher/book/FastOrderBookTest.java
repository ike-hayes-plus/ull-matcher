package io.github.ike.ullmatcher.book;

import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.core.Order;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FastOrderBookTest {
    @Test
    void hasFillableQuantityScansAcrossPriceLevelsInPriorityOrder() {
        FastOrderBook book = new FastOrderBook(8, 8);
        Order ask100 = order(1L, 10L, Side.SELL.code, 100L, 3L);
        Order ask101 = order(2L, 11L, Side.SELL.code, 101L, 2L);
        assertTrue(book.add(ask101));
        assertTrue(book.add(ask100));

        assertTrue(book.hasFillableQuantity(Side.BUY.code, 101L, 5L, 99L, false));
        assertFalse(book.hasFillableQuantity(Side.BUY.code, 100L, 5L, 99L, false));
        assertSame(ask100, book.bestAsk());
    }

    @Test
    void preventSelfTradeRejectsWhenSameUserAppearsBeforeFillCompletes() {
        FastOrderBook book = new FastOrderBook(8, 8);
        assertTrue(book.add(order(1L, 10L, Side.SELL.code, 100L, 1L)));
        assertTrue(book.add(order(2L, 42L, Side.SELL.code, 101L, 1L)));
        assertTrue(book.add(order(3L, 11L, Side.SELL.code, 102L, 2L)));

        assertFalse(book.hasFillableQuantity(Side.BUY.code, 102L, 3L, 42L, true));
        assertTrue(book.hasSelfTradeInFillPath(Side.BUY.code, 102L, 42L, 3L));
        assertFalse(book.hasSelfTradeInFillPath(Side.BUY.code, 100L, 42L, 1L));
    }

    private static Order order(long orderId, long userId, byte side, long price, long quantity) {
        Order order = new Order();
        order.orderId = orderId;
        order.userId = userId;
        order.symbolId = 1;
        order.side = side;
        order.price = price;
        order.quantity = quantity;
        order.remaining = quantity;
        order.sequence = orderId;
        return order;
    }
}
