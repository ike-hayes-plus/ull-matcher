package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.storage.snapshot.SnapshotStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class SnapshotCoordinator {
    private final Path snapshotFile;
    private final MatcherConfig matcherConfig;
    private final AtomicLong nextSequence;
    private final long offerTimeoutNanos;
    private final TtlCancelGuard ttlCancelGuard;
    private final OrderStateTracker orderStateTracker;

    SnapshotCoordinator(Path snapshotFile,
                        MatcherConfig matcherConfig,
                        AtomicLong nextSequence,
                        long offerTimeoutNanos,
                        TtlCancelGuard ttlCancelGuard,
                        OrderStateTracker orderStateTracker) {
        this.snapshotFile = snapshotFile;
        this.matcherConfig = matcherConfig;
        this.nextSequence = nextSequence;
        this.offerTimeoutNanos = offerTimeoutNanos;
        this.ttlCancelGuard = ttlCancelGuard;
        this.orderStateTracker = orderStateTracker;
    }

    RestoredSnapshot restore(MatchEventHandler eventHandler) throws IOException {
        if (!Files.exists(snapshotFile)) {
            return new RestoredSnapshot(new UltraLowLatencyMatcher(matcherConfig, eventHandler), 0L, null);
        }
        SnapshotStore.RestoreResult restore = SnapshotStore.restore(snapshotFile, matcherConfig, eventHandler);
        for (SnapshotStore.SnapshotLiveOrder order : SnapshotStore.scanLiveOrders(snapshotFile)) {
            ttlCancelGuard.onRecoveredLiveOrder(order.orderId(), order.timeInForce(), order.expireAtEpochMillis());
            orderStateTracker.onRecoveredLiveOrder(order);
        }
        SnapshotMaterial snapshot = new SnapshotMaterial(snapshotFile, restore.snapshotSequence(),
                restore.matcher().lastTradeId(), restore.matcher().liveOrderCount());
        return new RestoredSnapshot(restore.matcher(), restore.snapshotSequence(), snapshot);
    }

    SnapshotMaterial createSnapshot(MatcherEngine engine) throws IOException {
        long markerSequence = nextSequence.getAndIncrement();
        SubmitResult result = engine.gateway().trySubmit(
                Command.snapshotMarker(markerSequence, matcherConfig.symbolId()),
                offerTimeoutNanos
        );
        if (result != SubmitResult.ACCEPTED) {
            throw new IOException("failed to enqueue snapshot marker: " + result);
        }
        awaitSequenceApplied(engine.matcher(), markerSequence, engine.ring());
        SnapshotStore.SnapshotMetadata metadata = SnapshotStore.write(snapshotFile, engine.matcher());
        engine.standbySyncService().markSnapshot(metadata.lastSequence());
        return new SnapshotMaterial(metadata.file(), metadata.lastSequence(), metadata.lastTradeId(), metadata.liveOrderCount());
    }

    private static void awaitSequenceApplied(UltraLowLatencyMatcher matcher, long markerSequence, SpscRingBuffer<Command> ring)
            throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (matcher.lastSequence() < markerSequence || ring.size() > 0) {
            if (System.nanoTime() > deadline) {
                throw new IOException("timed out waiting for snapshot marker to drain");
            }
            Thread.onSpinWait();
        }
    }

    record RestoredSnapshot(UltraLowLatencyMatcher matcher, long snapshotSequence, SnapshotMaterial snapshotMaterial) {}
}
