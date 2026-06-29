package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.OrderStatus;
import io.github.ike.ullmatcher.api.RejectReason;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.storage.snapshot.SnapshotStore;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

final class OrderStateTracker implements MatchEventHandler {
    private final Clock clock;
    private final ConcurrentHashMap<Long, SubmittedOrder> submittedOrders = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, TrackedOrderState> states = new ConcurrentHashMap<>();
    private final ArrayDeque<Long> recentOrderIds = new ArrayDeque<>();
    private final Object recentLock = new Object();
    private final int recentLimit;

    OrderStateTracker(int recentLimit) {
        this(recentLimit, Clock.systemUTC());
    }

    OrderStateTracker(int recentLimit, Clock clock) {
        if (recentLimit <= 0) {
            throw new IllegalArgumentException("recentLimit must be positive");
        }
        this.recentLimit = recentLimit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    void onSubmissionAccepted(long orderId, Side side, OrderType orderType, TimeInForce tif, long price, long quantity) {
        submittedOrders.put(orderId, new SubmittedOrder(side.name(), orderType.name(), tif.name(), price, quantity));
    }

    void onRecoveredLiveOrder(SnapshotStore.SnapshotLiveOrder order) {
        TrackedOrderState state = new TrackedOrderState(order.orderId());
        state.sequence = order.sequence();
        state.symbolId = order.symbolId();
        state.status = order.remaining() == order.quantity() ? OrderStatus.NEW.name() : OrderStatus.PARTIALLY_FILLED.name();
        state.rejectReason = "NONE";
        state.remaining = order.remaining();
        state.side = Side.from(order.side()).name();
        state.orderType = OrderType.LIMIT.name();
        state.timeInForce = order.timeInForce().name();
        state.price = order.price();
        state.quantity = order.quantity();
        state.expireAtEpochMillis = order.expireAtEpochMillis();
        state.updatedAtEpochMillis = clock.millis();
        states.put(order.orderId(), state);
        remember(order.orderId());
    }

    OrderStateView find(long orderId) {
        TrackedOrderState state = states.get(orderId);
        return state == null ? null : state.snapshot();
    }

    List<OrderStateView> recent(int limit) {
        int effectiveLimit = Math.max(1, limit);
        List<OrderStateView> views = new ArrayList<>(effectiveLimit);
        HashSet<Long> seen = new HashSet<>(effectiveLimit * 2);
        synchronized (recentLock) {
            var iterator = recentOrderIds.descendingIterator();
            while (iterator.hasNext() && views.size() < effectiveLimit) {
                long orderId = iterator.next();
                if (!seen.add(orderId)) {
                    continue;
                }
                TrackedOrderState state = states.get(orderId);
                if (state != null) {
                    views.add(state.snapshot());
                }
            }
        }
        views.sort(Comparator.comparingLong(OrderStateView::updatedAtEpochMillis).reversed());
        return views;
    }

    @Override
    public void onTrade(TradeEvent event) {
    }

    @Override
    public void onOrder(OrderEvent event) {
        SubmittedOrder submitted = submittedOrders.get(event.orderId);
        TrackedOrderState state = states.get(event.orderId);
        boolean terminal = event.status == OrderStatus.FILLED || event.status == OrderStatus.CANCELLED ||
                event.status == OrderStatus.REJECTED;
        if (terminal && event.rejectReason == RejectReason.DUPLICATE_ORDER_ID &&
                state != null && isActive(state.status)) {
            submittedOrders.remove(event.orderId);
            return;
        }
        if (state == null) {
            TrackedOrderState created = new TrackedOrderState(event.orderId);
            TrackedOrderState raced = states.putIfAbsent(event.orderId, created);
            state = raced == null ? created : raced;
        }
        if (submitted != null) {
            state.side = submitted.side();
            state.orderType = submitted.orderType();
            state.timeInForce = submitted.timeInForce();
            state.price = submitted.price();
            state.quantity = submitted.quantity();
        }
        state.sequence = event.sequence;
        state.symbolId = event.symbolId;
        state.status = event.status.name();
        state.rejectReason = event.rejectReason.name();
        state.remaining = event.remaining;
        if (event.expireAtEpochMillis > 0L) {
            state.expireAtEpochMillis = event.expireAtEpochMillis;
        }
        state.updatedAtEpochMillis = clock.millis();
        remember(event.orderId);
        if (terminal) {
            submittedOrders.remove(event.orderId);
        }
    }

    private static boolean isActive(String status) {
        return OrderStatus.NEW.name().equals(status) || OrderStatus.PARTIALLY_FILLED.name().equals(status);
    }

    private void remember(long orderId) {
        synchronized (recentLock) {
            recentOrderIds.addLast(orderId);
            while (recentOrderIds.size() > recentLimit) {
                recentOrderIds.removeFirst();
            }
        }
    }

    private static final class TrackedOrderState {
        private final long orderId;
        private volatile long sequence;
        private volatile int symbolId;
        private volatile String status;
        private volatile String rejectReason;
        private volatile long remaining;
        private volatile String side;
        private volatile String orderType;
        private volatile String timeInForce;
        private volatile Long price;
        private volatile Long quantity;
        private volatile Long expireAtEpochMillis;
        private volatile long updatedAtEpochMillis;

        private TrackedOrderState(long orderId) {
            this.orderId = orderId;
        }

        private OrderStateView snapshot() {
            return new OrderStateView(
                    orderId,
                    sequence,
                    symbolId,
                    status,
                    rejectReason,
                    remaining,
                    side,
                    orderType,
                    timeInForce,
                    price,
                    quantity,
                    expireAtEpochMillis,
                    updatedAtEpochMillis
            );
        }
    }

    private record SubmittedOrder(String side, String orderType, String timeInForce, long price, long quantity) {
    }
}
