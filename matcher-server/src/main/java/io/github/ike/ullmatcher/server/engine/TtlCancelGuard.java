package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderStatus;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.api.TtlEvent;
import io.github.ike.ullmatcher.api.TtlEventAction;
import io.github.ike.ullmatcher.hft.SubmitResult;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

final class TtlCancelGuard implements MatchEventHandler, Closeable {
    private final TtlCancelConfig config;
    private final Clock clock;
    private final CancelExecutor cancelExecutor;
    private final int symbolId;
    private final ConcurrentHashMap<Long, PendingSubmission> pendingSubmissions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, TrackedOrder> trackedOrders = new ConcurrentHashMap<>();
    private final PriorityBlockingQueue<ExpiryEntry> expiryQueue = new PriorityBlockingQueue<>();
    private final ArrayDeque<TtlAuditEntry> recentAuditEntries = new ArrayDeque<>();
    private final Object auditLock = new Object();
    private final AtomicLong scheduledTotal = new AtomicLong();
    private final AtomicLong cancelRequestedTotal = new AtomicLong();
    private final AtomicLong cancelAcceptedTotal = new AtomicLong();
    private final AtomicLong cancelSkippedTotal = new AtomicLong();
    private final AtomicLong cancelFailedTotal = new AtomicLong();
    private volatile boolean running;
    private Thread reaperThread;
    private volatile MatchEventHandler ttlEventSink = NoopMatchEventHandler.INSTANCE;

    TtlCancelGuard(TtlCancelConfig config, CancelExecutor cancelExecutor, int symbolId) {
        this(config, cancelExecutor, symbolId, Clock.systemUTC());
    }

    TtlCancelGuard(TtlCancelConfig config, CancelExecutor cancelExecutor, int symbolId, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.cancelExecutor = Objects.requireNonNull(cancelExecutor, "cancelExecutor");
        this.symbolId = symbolId;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    void bindEventSink(MatchEventHandler ttlEventSink) {
        this.ttlEventSink = Objects.requireNonNull(ttlEventSink, "ttlEventSink");
    }

    void start() {
        if (!config.enabled() || running) {
            return;
        }
        running = true;
        reaperThread = Thread.ofPlatform().name("ttl-guard").start(this::runReaper);
    }

    void onSubmissionAccepted(long orderId, TimeInForce tif, long expireAtEpochMillis) {
        if (!config.enabled()) {
            return;
        }
        pendingSubmissions.put(orderId, new PendingSubmission(orderId, tif, expireAtEpochMillis, clock.millis()));
    }

    void onRecoveredLiveOrder(long orderId, TimeInForce tif, long persistedExpireAtEpochMillis) {
        if (!config.enabled() || (tif != TimeInForce.GTC && tif != TimeInForce.POST_ONLY)) {
            return;
        }
        long expireAtMillis = config.resolveRecoveredExpireAtEpochMillis(persistedExpireAtEpochMillis, clock.millis());
        if (expireAtMillis <= 0L) {
            return;
        }
        TrackedOrder tracked = trackedOrders.compute(orderId, (ignored, existing) -> {
            long version = existing == null ? 1L : existing.version() + 1L;
            return new TrackedOrder(orderId, version, expireAtMillis, "RECOVERED");
        });
        expiryQueue.offer(new ExpiryEntry(tracked.orderId(), tracked.version(), tracked.expireAtEpochMillis()));
        scheduledTotal.incrementAndGet();
        audit("SCHEDULED", tracked.orderId(), "RECOVERED", "expireAt=" + expireAtMillis, expireAtMillis);
        emitEvent(TtlEventAction.SCHEDULED, tracked.orderId(), tracked.source(), "expireAt=" + expireAtMillis, expireAtMillis);
    }

    @Override
    public void onTrade(TradeEvent event) {
    }

    @Override
    public void onOrder(OrderEvent event) {
        if (!config.enabled()) {
            return;
        }
        PendingSubmission pending = pendingSubmissions.remove(event.orderId);
        if (event.status == OrderStatus.NEW || event.status == OrderStatus.PARTIALLY_FILLED) {
            long expireAtMillis = pending != null ? pending.expireAtEpochMillis() : event.expireAtEpochMillis;
            if (expireAtMillis > 0L && event.remaining > 0L) {
                TrackedOrder tracked = trackedOrders.compute(event.orderId, (orderId, existing) -> {
                    long version = existing == null ? 1L : existing.version() + 1L;
                    return new TrackedOrder(orderId, version, expireAtMillis, pending == null ? "RECOVERED" : "REQUESTED");
                });
                expiryQueue.offer(new ExpiryEntry(tracked.orderId(), tracked.version(), tracked.expireAtEpochMillis()));
                scheduledTotal.incrementAndGet();
                audit("SCHEDULED", tracked.orderId(), tracked.source(), "expireAt=" + expireAtMillis, expireAtMillis);
                emitEvent(TtlEventAction.SCHEDULED, tracked.orderId(), tracked.source(), "expireAt=" + expireAtMillis, expireAtMillis);
            }
            return;
        }
        trackedOrders.remove(event.orderId);
        if (pending != null) {
            audit("REMOVED", event.orderId, "REQUESTED", event.status.name(), 0L);
            emitEvent(TtlEventAction.REMOVED, event.orderId, "REQUESTED", event.status.name(), 0L);
        }
    }

    TtlMetricsSnapshot snapshot() {
        synchronized (auditLock) {
            return new TtlMetricsSnapshot(
                    config.enabled(),
                    trackedOrders.size(),
                    pendingSubmissions.size(),
                    scheduledTotal.get(),
                    cancelRequestedTotal.get(),
                    cancelAcceptedTotal.get(),
                    cancelSkippedTotal.get(),
                    cancelFailedTotal.get(),
                    List.copyOf(recentAuditEntries)
            );
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (reaperThread != null) {
            reaperThread.interrupt();
            try {
                reaperThread.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping ttl guard", e);
            }
        }
        pendingSubmissions.clear();
        trackedOrders.clear();
        expiryQueue.clear();
    }

    private void runReaper() {
        while (running) {
            try {
                sweep(clock.millis());
                Thread.sleep(config.sweepIntervalMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                audit("REAPER_ERROR", 0L, "SYSTEM_TTL_GUARD", e.getClass().getSimpleName() + ": " + e.getMessage(), 0L);
            }
        }
    }

    void sweep(long nowMillis) {
        while (true) {
            ExpiryEntry head = expiryQueue.peek();
            if (head == null || head.expireAtEpochMillis() > nowMillis) {
                return;
            }
            expiryQueue.poll();
            TrackedOrder tracked = trackedOrders.get(head.orderId());
            if (tracked == null || tracked.version() != head.version() || tracked.expireAtEpochMillis() != head.expireAtEpochMillis()) {
                continue;
            }
            cancelRequestedTotal.incrementAndGet();
            audit("CANCEL_REQUESTED", tracked.orderId(), "SYSTEM_TTL_GUARD", "expired", tracked.expireAtEpochMillis());
            emitEvent(TtlEventAction.EXPIRED, tracked.orderId(), "SYSTEM_TTL_GUARD", "expired", tracked.expireAtEpochMillis());
            emitEvent(TtlEventAction.CANCEL_REQUESTED, tracked.orderId(), "SYSTEM_TTL_GUARD", "expired", tracked.expireAtEpochMillis());
            try {
                SubmitResult result = cancelExecutor.cancel(tracked.orderId());
                if (result == SubmitResult.ACCEPTED) {
                    cancelAcceptedTotal.incrementAndGet();
                    trackedOrders.remove(tracked.orderId(), tracked);
                    audit("CANCEL_ACCEPTED", tracked.orderId(), "SYSTEM_TTL_GUARD", result.name(), tracked.expireAtEpochMillis());
                    emitEvent(TtlEventAction.CANCEL_ACCEPTED, tracked.orderId(), "SYSTEM_TTL_GUARD", result.name(), tracked.expireAtEpochMillis());
                } else if (result.walAppended()) {
                    cancelSkippedTotal.incrementAndGet();
                    audit("CANCEL_PENDING_RECOVERY", tracked.orderId(), "SYSTEM_TTL_GUARD", result.name(), tracked.expireAtEpochMillis());
                    emitEvent(TtlEventAction.CANCEL_PENDING_RECOVERY, tracked.orderId(), "SYSTEM_TTL_GUARD", result.name(), tracked.expireAtEpochMillis());
                    reschedule(tracked, "RECOVERY_RETRY");
                } else {
                    cancelSkippedTotal.incrementAndGet();
                    audit("CANCEL_SKIPPED", tracked.orderId(), "SYSTEM_TTL_GUARD", result.name(), tracked.expireAtEpochMillis());
                    emitEvent(TtlEventAction.CANCEL_SKIPPED, tracked.orderId(), "SYSTEM_TTL_GUARD", result.name(), tracked.expireAtEpochMillis());
                    reschedule(tracked, "RETRY");
                }
            } catch (Exception e) {
                cancelFailedTotal.incrementAndGet();
                audit("CANCEL_FAILED", tracked.orderId(), "SYSTEM_TTL_GUARD", e.getClass().getSimpleName() + ": " + e.getMessage(),
                        tracked.expireAtEpochMillis());
                emitEvent(TtlEventAction.CANCEL_FAILED, tracked.orderId(), "SYSTEM_TTL_GUARD",
                        e.getClass().getSimpleName() + ": " + e.getMessage(), tracked.expireAtEpochMillis());
                reschedule(tracked, "ERROR_RETRY");
            }
        }
    }

    private void audit(String action, long orderId, String source, String detail, long expireAtMillis) {
        if (config.recentAuditLimit() <= 0) {
            return;
        }
        synchronized (auditLock) {
            while (recentAuditEntries.size() >= config.recentAuditLimit()) {
                recentAuditEntries.removeFirst();
            }
            recentAuditEntries.addLast(new TtlAuditEntry(clock.millis(), orderId, action, source, detail, expireAtMillis));
        }
    }

    private void reschedule(TrackedOrder tracked, String source) {
        long nextExpireAtMillis = clock.millis() + Math.max(1L, config.sweepIntervalMillis());
        TrackedOrder rescheduled = trackedOrders.computeIfPresent(tracked.orderId(), (ignored, current) -> {
            if (current.version() != tracked.version()) {
                return current;
            }
            return new TrackedOrder(current.orderId(), current.version() + 1L, nextExpireAtMillis, source);
        });
        if (rescheduled != null && rescheduled.version() != tracked.version()) {
            expiryQueue.offer(new ExpiryEntry(rescheduled.orderId(), rescheduled.version(), rescheduled.expireAtEpochMillis()));
            scheduledTotal.incrementAndGet();
            audit("RESCHEDULED", rescheduled.orderId(), source, "expireAt=" + nextExpireAtMillis, nextExpireAtMillis);
            emitEvent(TtlEventAction.RESCHEDULED, rescheduled.orderId(), source, "expireAt=" + nextExpireAtMillis, nextExpireAtMillis);
        }
    }

    private void emitEvent(TtlEventAction action, long orderId, String source, String detail, long expireAtMillis) {
        TtlEvent event = new TtlEvent();
        event.eventTimeEpochMillis = clock.millis();
        event.symbolId = symbolId;
        event.orderId = orderId;
        event.action = action;
        event.source = source;
        event.detail = detail;
        event.expireAtEpochMillis = expireAtMillis;
        ttlEventSink.onTtl(event);
    }

    @FunctionalInterface
    interface CancelExecutor {
        SubmitResult cancel(long orderId) throws IOException;
    }

    private enum NoopMatchEventHandler implements MatchEventHandler {
        INSTANCE;

        @Override
        public void onTrade(TradeEvent event) {
        }

        @Override
        public void onOrder(OrderEvent event) {
        }
    }

    private record PendingSubmission(long orderId, TimeInForce tif, long expireAtEpochMillis, long acceptedAtEpochMillis) {
    }

    private record TrackedOrder(long orderId, long version, long expireAtEpochMillis, String source) {
    }

    private record ExpiryEntry(long orderId, long version, long expireAtEpochMillis) implements Comparable<ExpiryEntry> {
        @Override
        public int compareTo(ExpiryEntry other) {
            return Long.compare(expireAtEpochMillis, other.expireAtEpochMillis);
        }
    }
}
