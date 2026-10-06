package io.github.ike.ullmatcher.api;

import io.github.ike.ullmatcher.core.MatcherConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Wire-code round trips for the enums that cross the binary ingress and WAL boundaries.
 * An unmapped code must be rejected; this tree does not decode unknown wire values.
 */
final class WireCodecTest {
    @Test
    void timeInForceRoundTripsEveryDeclaredCode() {
        for (TimeInForce value : TimeInForce.values()) {
            assertEquals(value, TimeInForce.from(value.code));
        }
    }

    @Test
    void unknownWireCodesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> TimeInForce.from((byte) 0));
        assertThrows(IllegalArgumentException.class, () -> TimeInForce.from((byte) 99));
        assertThrows(IllegalArgumentException.class, () -> Side.from((byte) 0));
        assertThrows(IllegalArgumentException.class, () -> OrderType.from((byte) 0));
    }

    @Test
    void sideRoundTripsEveryDeclaredCode() {
        for (Side value : Side.values()) {
            assertEquals(value, Side.from(value.code));
        }
    }

    @Test
    void orderTypeRoundTripsEveryDeclaredCode() {
        for (OrderType value : OrderType.values()) {
            assertEquals(value, OrderType.from(value.code));
        }
    }

    @Test
    void commandTypeOrdinalsAreTheWalWireCodes() {
        assertEquals(0, CommandType.NEW_ORDER.ordinal());
        assertEquals(1, CommandType.CANCEL_ORDER.ordinal());
        assertEquals(2, CommandType.SNAPSHOT_MARKER.ordinal());
        assertEquals(3, CommandType.SHUTDOWN.ordinal());
        assertEquals(4, CommandType.values().length);
    }

    @Test
    void ttlEventActionsAreStableAcrossRestarts() {
        // Persisted audit records reference these names; renaming one silently breaks replay tooling.
        assertEquals(9, TtlEventAction.values().length);
        for (TtlEventAction action : TtlEventAction.values()) {
            assertEquals(action, TtlEventAction.valueOf(action.name()));
        }
    }

    @Test
    void matcherConfigDefaultsMatchDocumentedConstants() {
        MatcherConfig config = MatcherConfig.defaults(7);

        assertEquals(7, config.symbolId());
        assertEquals(MatcherConfig.DEFAULT_EXPECTED_PRICE_LEVELS, config.expectedPriceLevels());
        assertEquals(MatcherConfig.DEFAULT_EXPECTED_LIVE_ORDERS, config.expectedLiveOrders());
        assertEquals(MatcherConfig.DEFAULT_ORDER_POOL_SIZE, config.orderPoolSize());
        assertEquals(MatcherConfig.DEFAULT_QUOTE_SCALE, config.quoteScale());
        assertEquals(MatcherConfig.DEFAULT_PREVENT_SELF_TRADE, config.preventSelfTrade());
    }

    @Test
    void matcherConfigRejectsNonPositiveCapacities() {
        assertThrows(IllegalArgumentException.class, () -> new MatcherConfig(0, 1, 1, 1, 1L, true));
        assertThrows(IllegalArgumentException.class, () -> new MatcherConfig(1, 0, 1, 1, 1L, true));
        assertThrows(IllegalArgumentException.class, () -> new MatcherConfig(1, 1, 0, 1, 1L, true));
        assertThrows(IllegalArgumentException.class, () -> new MatcherConfig(1, 1, 1, 0, 1L, true));
        assertThrows(IllegalArgumentException.class, () -> new MatcherConfig(1, 1, 1, 1, 0L, true));
    }

    @Test
    void reusableEventSlotsRenderDiagnostics() {
        TradeEvent trade = new TradeEvent();
        trade.sequence = 1L;
        trade.tradeId = 2L;
        trade.price = 100L;
        trade.quantity = 5L;
        assertNotNull(trade.toString());

        OrderEvent order = new OrderEvent();
        order.sequence = 1L;
        order.status = OrderStatus.NEW;
        assertEquals(RejectReason.NONE, order.rejectReason, "reject reason must default to NONE");
        assertNotNull(order.toString());

        TtlEvent ttl = new TtlEvent();
        ttl.action = TtlEventAction.SCHEDULED;
        assertEquals(TtlEventAction.SCHEDULED, ttl.action);
    }

    @Test
    void matchEventHandlerDefaultsIgnoreTtlEvents() {
        MatchEventHandler handler = new MatchEventHandler() {
            @Override
            public void onTrade(TradeEvent event) {}

            @Override
            public void onOrder(OrderEvent event) {}
        };

        handler.onTtl(new TtlEvent());
    }
}
