package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderStatus;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.RejectReason;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class OrderStateTrackerTest {
    @Test
    void duplicateOrderIdRejectionDoesNotOverwriteExistingLiveOrderState() {
        OrderStateTracker tracker = new OrderStateTracker(16);
        tracker.onSubmissionAccepted(101L, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 5L);

        OrderEvent accepted = new OrderEvent();
        accepted.sequence = 1L;
        accepted.symbolId = 1;
        accepted.orderId = 101L;
        accepted.status = OrderStatus.NEW;
        accepted.rejectReason = RejectReason.NONE;
        accepted.remaining = 5L;
        tracker.onOrder(accepted);

        tracker.onSubmissionAccepted(101L, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 99L, 2L);
        OrderEvent duplicate = new OrderEvent();
        duplicate.sequence = 2L;
        duplicate.symbolId = 1;
        duplicate.orderId = 101L;
        duplicate.status = OrderStatus.REJECTED;
        duplicate.rejectReason = RejectReason.DUPLICATE_ORDER_ID;
        duplicate.remaining = 2L;
        tracker.onOrder(duplicate);

        OrderStateView state = tracker.find(101L);
        assertEquals(1L, state.sequence());
        assertEquals("NEW", state.status());
        assertEquals("NONE", state.rejectReason());
        assertEquals("BUY", state.side());
        assertEquals(100L, state.price());
        assertEquals(5L, state.quantity());
        assertEquals(5L, state.remaining());
    }
}
