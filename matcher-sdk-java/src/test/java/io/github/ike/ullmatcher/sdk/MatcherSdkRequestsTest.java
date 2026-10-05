package io.github.ike.ullmatcher.sdk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class MatcherSdkRequestsTest {
    @Test
    void limitFactoryFillsOrderTypeAndLeavesTtlUnset() {
        NewOrderRequest request = NewOrderRequest.limit(7L, 101L, "BUY", "GTC", 12L, 3L, "k1");

        assertEquals("LIMIT", request.orderType());
        assertNull(request.ttlMillis());
        assertEquals("k1", request.idempotencyKey());
    }

    @Test
    void newOrderRejectsInvalidNumericFields() {
        assertThrows(IllegalArgumentException.class, () -> NewOrderRequest.limit(0L, 101L, "BUY", "GTC", 12L, 3L, null));
        assertThrows(IllegalArgumentException.class, () -> NewOrderRequest.limit(7L, 0L, "BUY", "GTC", 12L, 3L, null));
        assertThrows(IllegalArgumentException.class, () -> NewOrderRequest.limit(7L, 101L, "BUY", "GTC", -1L, 3L, null));
        assertThrows(IllegalArgumentException.class, () -> NewOrderRequest.limit(7L, 101L, "BUY", "GTC", 12L, 0L, null));
    }

    @Test
    void newOrderRejectsBlankEnumerationFields() {
        assertThrows(IllegalArgumentException.class, () -> NewOrderRequest.limit(7L, 101L, null, "GTC", 12L, 3L, null));
        assertThrows(IllegalArgumentException.class, () -> NewOrderRequest.limit(7L, 101L, " ", "GTC", 12L, 3L, null));
        assertThrows(IllegalArgumentException.class, () -> NewOrderRequest.limit(7L, 101L, "BUY", " ", 12L, 3L, null));
        assertThrows(IllegalArgumentException.class,
                () -> new NewOrderRequest(7L, 101L, "BUY", null, "GTC", 12L, 3L, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new NewOrderRequest(7L, 101L, "BUY", "LIMIT", null, 12L, 3L, null, null));
    }

    @Test
    void cancelOrderRequestRequiresPositiveOrderId() {
        CancelOrderRequest request = new CancelOrderRequest(101L, "k1");

        assertEquals(101L, request.orderId());
        assertEquals("k1", request.idempotencyKey());
        assertThrows(IllegalArgumentException.class, () -> new CancelOrderRequest(0L, "k1"));
        assertThrows(IllegalArgumentException.class, () -> new CancelOrderRequest(-1L, null));
    }

    @Test
    void buyLimitFactoryUsesWireCodesAndUnsetTtl() {
        BinaryNewOrder order = BinaryNewOrder.buyLimit(1L, 101L, 99L, 2L);

        assertEquals((byte) 'B', order.side());
        assertEquals((byte) 'L', order.orderType());
        assertEquals((byte) 'G', order.timeInForce());
        assertEquals(-1L, order.ttlMillis());
    }

    @Test
    void binaryNewOrderRejectsInvalidNumericFields() {
        assertThrows(IllegalArgumentException.class, () -> BinaryNewOrder.buyLimit(0L, 101L, 99L, 2L));
        assertThrows(IllegalArgumentException.class, () -> BinaryNewOrder.buyLimit(1L, 0L, 99L, 2L));
        assertThrows(IllegalArgumentException.class, () -> BinaryNewOrder.buyLimit(1L, 101L, -1L, 2L));
        assertThrows(IllegalArgumentException.class, () -> BinaryNewOrder.buyLimit(1L, 101L, 99L, 0L));
    }

    @Test
    void statusCarryingExceptionKeepsStatusAndBody() {
        MatcherClientException error = new MatcherClientException("failed", 409, "{\"error\":\"conflict\"}");

        assertEquals("failed", error.getMessage());
        assertEquals(409, error.statusCode());
        assertEquals("{\"error\":\"conflict\"}", error.responseBody());
        assertNull(error.getCause());
    }

    @Test
    void transportExceptionUsesSentinelStatusAndEmptyBody() {
        RuntimeException cause = new RuntimeException("socket closed");
        MatcherClientException error = new MatcherClientException("transport failure", cause);

        assertEquals(-1, error.statusCode());
        assertEquals("", error.responseBody());
        assertSame(cause, error.getCause());
    }
}
