/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderStatus;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.RejectReason;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.storage.snapshot.SnapshotStore;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ServerEngineBranchCoverageTest {
    @Test
    void ttlCancelConfigRejectsNegativeValuesAndDisabledSweep() {
        assertThrows(IllegalArgumentException.class, () -> new TtlCancelConfig(false, -1L, 0L, 0L, 0L, 0));
        assertThrows(IllegalArgumentException.class, () -> new TtlCancelConfig(false, 0L, -1L, 0L, 0L, 0));
        assertThrows(IllegalArgumentException.class, () -> new TtlCancelConfig(false, 0L, 0L, -1L, 0L, 0));
        assertThrows(IllegalArgumentException.class, () -> new TtlCancelConfig(false, 0L, 0L, 0L, -1L, 0));
        assertThrows(IllegalArgumentException.class, () -> new TtlCancelConfig(false, 0L, 0L, 0L, 0L, -1));
        assertThrows(IllegalArgumentException.class, () -> new TtlCancelConfig(true, 0L, 10L, 10L, 10L, 1));
        assertEquals(0L, TtlCancelConfig.disabled().sweepIntervalMillis());
        assertTrue(TtlCancelConfig.defaults().hardTtlMillis() > 0L);
    }

    @Test
    void ttlCancelConfigResolvesExpireAndRecoveredPaths() {
        TtlCancelConfig disabled = TtlCancelConfig.disabled();
        assertEquals(0L, disabled.resolveExpireAtEpochMillis(TimeInForce.GTC, 10L, 1_000L));
        assertEquals(0L, disabled.resolveRecoveredExpireAtEpochMillis(50L, 1_000L));

        TtlCancelConfig enabled = new TtlCancelConfig(true, 10L, 20L, 50L, 30L, 8);
        assertEquals(0L, enabled.resolveExpireAtEpochMillis(TimeInForce.GTC, 10L, 0L));
        assertEquals(0L, enabled.resolveExpireAtEpochMillis(TimeInForce.IOC, 10L, 1_000L));
        assertEquals(0L, enabled.resolveExpireAtEpochMillis(TimeInForce.FOK, 10L, 1_000L));
        assertEquals(1_010L, enabled.resolveExpireAtEpochMillis(TimeInForce.GTC, 10L, 1_000L));
        assertEquals(1_020L, enabled.resolveExpireAtEpochMillis(TimeInForce.GTC, null, 1_000L));
        assertEquals(1_020L, enabled.resolveExpireAtEpochMillis(TimeInForce.GTC, 0L, 1_000L));
        assertEquals(1_050L, enabled.resolveExpireAtEpochMillis(TimeInForce.GTC, 80L, 1_000L));
        assertEquals(50L, enabled.resolveRecoveredExpireAtEpochMillis(50L, 1_000L));
        assertEquals(1_030L, enabled.resolveRecoveredExpireAtEpochMillis(0L, 1_000L));

        TtlCancelConfig hardOnly = new TtlCancelConfig(true, 10L, 0L, 40L, 0L, 1);
        assertEquals(1_040L, hardOnly.resolveExpireAtEpochMillis(TimeInForce.POST_ONLY, null, 1_000L));
        assertEquals(1_040L, hardOnly.resolveRecoveredExpireAtEpochMillis(0L, 1_000L));

        TtlCancelConfig noTtl = new TtlCancelConfig(true, 10L, 0L, 0L, 0L, 1);
        assertEquals(0L, noTtl.resolveExpireAtEpochMillis(TimeInForce.GTC, null, 1_000L));
        assertEquals(0L, noTtl.resolveRecoveredExpireAtEpochMillis(0L, 1_000L));
    }

    @Test
    void orderStateTrackerCoversRecentRecoveryAndMetadataBranches() {
        assertThrows(IllegalArgumentException.class, () -> new OrderStateTracker(0));
        OrderStateTracker tracker = new OrderStateTracker(2);

        tracker.onRecoveredLiveOrder(new SnapshotStore.SnapshotLiveOrder(
                1L, 7, Side.BUY.code, TimeInForce.GTC, 100L, 10L, 10L, 5L, 0L));
        tracker.onRecoveredLiveOrder(new SnapshotStore.SnapshotLiveOrder(
                2L, 7, Side.SELL.code, TimeInForce.GTC, 99L, 8L, 3L, 6L, 40L));

        OrderEvent orphanFilled = event(99L, OrderStatus.FILLED, RejectReason.NONE);
        tracker.onOrder(orphanFilled);
        assertNull(tracker.find(99L));

        OrderEvent recovered = event(3L, OrderStatus.NEW, RejectReason.NONE);
        recovered.side = Side.BUY.code;
        recovered.orderType = OrderType.LIMIT.code;
        recovered.timeInForce = TimeInForce.GTC.code;
        recovered.price = 11L;
        recovered.quantity = 4L;
        recovered.remaining = 4L;
        recovered.expireAtEpochMillis = 80L;
        tracker.onOrder(recovered);
        assertEquals("NEW", tracker.find(3L).status());
        assertEquals("BUY", tracker.find(3L).side());

        tracker.onOrder(event(3L, OrderStatus.FILLED, RejectReason.NONE));
        assertEquals("FILLED", tracker.find(3L).status());

        tracker.onSubmissionAccepted(4L, Side.SELL, OrderType.LIMIT, TimeInForce.POST_ONLY, 12L, 2L);
        tracker.onOrder(event(4L, OrderStatus.NEW, RejectReason.NONE));
        tracker.onOrder(event(4L, OrderStatus.CANCELLED, RejectReason.NONE));
        assertEquals("CANCELLED", tracker.find(4L).status());

        List<OrderStateView> recent = tracker.recent(0);
        assertFalseEmptyOrHasItems(recent);
        assertTrue(tracker.recent(1).size() <= 1);
        assertTrue(tracker.recent(8).size() >= 1);

        tracker.reset();
        assertNull(tracker.find(1L));
        assertTrue(tracker.recent(4).isEmpty());
    }

    @Test
    void ttlCancelGuardCoversDisabledRecoverySkipAndFailurePaths() throws Exception {
        TtlCancelConfig disabled = TtlCancelConfig.disabled();
        try (TtlCancelGuard guard = new TtlCancelGuard(disabled, orderId -> SubmitResult.ACCEPTED, 1)) {
            guard.start();
            guard.onSubmissionAccepted(1L, TimeInForce.GTC, 10L);
            guard.onRecoveredLiveOrder(2L, TimeInForce.GTC, 10L);
            guard.onOrder(event(1L, OrderStatus.NEW, RejectReason.NONE));
            assertEquals(0L, guard.snapshot().activeTrackedOrders());
        }

        AtomicReference<SubmitResult> next = new AtomicReference<>(SubmitResult.MATCHER_NOT_RUNNING);
        ManualClock clock = new ManualClock();
        TtlCancelConfig enabled = new TtlCancelConfig(true, 10L, 10L, 100L, 0L, 2);
        try (TtlCancelGuard guard = new TtlCancelGuard(enabled, orderId -> {
            SubmitResult result = next.get();
            if (result == null) {
                throw new IOException("cancel failed");
            }
            return result;
        }, 1, clock)) {
            guard.onRecoveredLiveOrder(8L, TimeInForce.IOC, 10L);
            guard.onRecoveredLiveOrder(9L, TimeInForce.GTC, 0L);
            guard.onRecoveredLiveOrder(10L, TimeInForce.GTC, 15L);
            assertEquals(2L, guard.snapshot().activeTrackedOrders());

            clock.setMillis(15L);
            guard.sweep(clock.millis());
            assertEquals(1L, guard.snapshot().cancelSkippedTotal());

            next.set(null);
            clock.setMillis(30L);
            guard.sweep(clock.millis());
            assertEquals(1L, guard.snapshot().cancelFailedTotal());

            guard.onSubmissionAccepted(11L, TimeInForce.GTC, 40L);
            OrderEvent rejected = event(11L, OrderStatus.REJECTED, RejectReason.NONE);
            guard.onOrder(rejected);

            OrderEvent live = event(12L, OrderStatus.PARTIALLY_FILLED, RejectReason.NONE);
            live.expireAtEpochMillis = 50L;
            live.remaining = 2L;
            guard.onOrder(live);
            guard.onRecoveredLiveOrder(13L, TimeInForce.POST_ONLY, 80L);
            clock.setMillis(5L);
            guard.sweep(clock.millis());
            guard.resetTrackedState();
            assertEquals(0L, guard.snapshot().activeTrackedOrders());
        }

        TtlCancelConfig noAudit = new TtlCancelConfig(true, 10L, 10L, 10L, 10L, 0);
        try (TtlCancelGuard guard = new TtlCancelGuard(noAudit, orderId -> SubmitResult.ACCEPTED, 1, new ManualClock())) {
            guard.onRecoveredLiveOrder(21L, TimeInForce.GTC, 1L);
            assertTrue(guard.snapshot().recentAuditEntries().isEmpty());
        }
    }

    private static void assertFalseEmptyOrHasItems(List<OrderStateView> recent) {
        assertTrue(recent.size() >= 1);
    }

    private static OrderEvent event(long orderId, OrderStatus status, RejectReason reason) {
        OrderEvent event = new OrderEvent();
        event.orderId = orderId;
        event.sequence = orderId;
        event.symbolId = 1;
        event.status = status;
        event.rejectReason = reason;
        event.remaining = status == OrderStatus.FILLED ? 0L : 1L;
        return event;
    }

    private static final class ManualClock extends Clock {
        private long millis;

        private void setMillis(long value) {
            this.millis = value;
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
