package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderStatus;
import io.github.ike.ullmatcher.api.RejectReason;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TtlEvent;
import io.github.ike.ullmatcher.api.TtlEventAction;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.hft.SubmitResult;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TtlCancelGuardTest {
    @Test
    void walAppendedButNotAppliedCancelRemainsTrackedAndRetriesUntilAccepted() throws Exception {
        AtomicReference<SubmitResult> nextCancelResult = new AtomicReference<>(SubmitResult.RING_FULL_AFTER_WAL_APPEND);
        TtlCancelConfig config = new TtlCancelConfig(true, 10L, 10L, 100L, 100L, 16);
        List<TtlEventAction> actions = new ArrayList<>();
        ManualClock clock = new ManualClock();
        try (TtlCancelGuard guard = new TtlCancelGuard(config, orderId -> nextCancelResult.get(), 1, clock)) {
            guard.bindEventSink(new EventSink(actions));
            guard.onSubmissionAccepted(42L, TimeInForce.GTC, 10L);
            guard.onOrder(newOrderEvent(42L, 1L));

            clock.setMillis(10L);
            guard.sweep(clock.millis());
            assertEquals(1L, guard.snapshot().cancelSkippedTotal());
            assertEquals(1L, guard.snapshot().activeTrackedOrders());
            assertTrue(guard.snapshot().recentAuditEntries().stream()
                    .anyMatch(entry -> "CANCEL_PENDING_RECOVERY".equals(entry.action())));

            nextCancelResult.set(SubmitResult.ACCEPTED);

            clock.setMillis(20L);
            guard.sweep(clock.millis());
            assertEquals(1L, guard.snapshot().cancelAcceptedTotal());
            assertEquals(0L, guard.snapshot().activeTrackedOrders());
            assertTrue(guard.snapshot().recentAuditEntries().stream()
                    .anyMatch(entry -> "RESCHEDULED".equals(entry.action())));
            assertTrue(actions.contains(TtlEventAction.EXPIRED));
            assertTrue(actions.contains(TtlEventAction.CANCEL_PENDING_RECOVERY));
            assertTrue(actions.contains(TtlEventAction.CANCEL_ACCEPTED));
        }
    }

    private record EventSink(List<TtlEventAction> actions) implements io.github.ike.ullmatcher.api.MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {
        }

        @Override
        public void onOrder(OrderEvent event) {
        }

        @Override
        public void onTtl(TtlEvent event) {
            actions.add(event.action);
        }
    }

    private static OrderEvent newOrderEvent(long orderId, long sequence) {
        OrderEvent event = new OrderEvent();
        event.orderId = orderId;
        event.sequence = sequence;
        event.symbolId = 1;
        event.status = OrderStatus.NEW;
        event.rejectReason = RejectReason.NONE;
        event.remaining = 1L;
        return event;
    }

    private static final class ManualClock extends Clock {
        private long millis;

        void setMillis(long millis) {
            this.millis = millis;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }
    }
}
