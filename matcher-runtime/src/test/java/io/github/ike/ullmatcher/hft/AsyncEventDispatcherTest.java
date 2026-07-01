package io.github.ike.ullmatcher.hft;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderStatus;
import io.github.ike.ullmatcher.api.RejectReason;
import io.github.ike.ullmatcher.api.TtlEvent;
import io.github.ike.ullmatcher.api.TtlEventAction;
import io.github.ike.ullmatcher.api.TradeEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class AsyncEventDispatcherTest {
    @Test
    void drainsTradeOrderAndTtlEventsInOrder() {
        AsyncEventDispatcher dispatcher = new AsyncEventDispatcher(4);
        dispatcher.onTrade(tradeEvent());
        dispatcher.onOrder(orderEvent());
        dispatcher.onTtl(ttlEvent());
        RecordingHandler handler = new RecordingHandler();

        assertEquals(2, dispatcher.drainTo(handler, 2));
        assertEquals(1, dispatcher.size());
        assertEquals(2, handler.events.size());
        assertEquals("trade:11:101:202:1000", handler.events.get(0));
        assertEquals("order:12:303:PARTIALLY_FILLED:7", handler.events.get(1));

        assertEquals(1, dispatcher.drainTo(handler, 10));
        assertEquals(0, dispatcher.size());
        assertEquals("ttl:13:404:CANCEL_ACCEPTED:expired", handler.events.get(2));
    }

    @Test
    void fullQueueRejectsWithoutLosingFailureMetric() {
        AsyncEventDispatcher dispatcher = new AsyncEventDispatcher(2);
        dispatcher.onTrade(tradeEvent());
        dispatcher.onOrder(orderEvent());

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> dispatcher.onTtl(ttlEvent()));

        assertEquals("match event outbox is full", error.getMessage());
        assertEquals(1L, dispatcher.fullFailureCount());
        assertEquals(2, dispatcher.size());
    }

    @Test
    void rejectsNonPowerOfTwoCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new AsyncEventDispatcher(3));
    }

    private static TradeEvent tradeEvent() {
        TradeEvent event = new TradeEvent();
        event.sequence = 11L;
        event.tradeId = 101L;
        event.symbolId = 1;
        event.buyOrderId = 202L;
        event.sellOrderId = 203L;
        event.buyerUserId = 301L;
        event.sellerUserId = 302L;
        event.price = 100L;
        event.quantity = 10L;
        event.quoteAmount = 1_000L;
        return event;
    }

    private static OrderEvent orderEvent() {
        OrderEvent event = new OrderEvent();
        event.sequence = 12L;
        event.symbolId = 1;
        event.orderId = 303L;
        event.status = OrderStatus.PARTIALLY_FILLED;
        event.rejectReason = RejectReason.NONE;
        event.remaining = 7L;
        event.expireAtEpochMillis = 123_456L;
        return event;
    }

    private static TtlEvent ttlEvent() {
        TtlEvent event = new TtlEvent();
        event.eventTimeEpochMillis = 13L;
        event.symbolId = 1;
        event.orderId = 404L;
        event.action = TtlEventAction.CANCEL_ACCEPTED;
        event.source = "ttl-guard";
        event.detail = "expired";
        event.expireAtEpochMillis = 123_999L;
        return event;
    }

    private static final class RecordingHandler implements MatchEventHandler {
        private final List<String> events = new ArrayList<>();

        @Override
        public void onTrade(TradeEvent event) {
            events.add("trade:" + event.sequence + ':' + event.tradeId + ':' + event.buyOrderId + ':' + event.quoteAmount);
        }

        @Override
        public void onOrder(OrderEvent event) {
            events.add("order:" + event.sequence + ':' + event.orderId + ':' + event.status + ':' + event.remaining);
        }

        @Override
        public void onTtl(TtlEvent event) {
            events.add("ttl:" + event.eventTimeEpochMillis + ':' + event.orderId + ':' + event.action + ':' + event.detail);
        }
    }
}
