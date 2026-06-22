package io.github.ike.ullmatcher.ha.standby;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StandbySyncServiceTest {
    @Test
    void replicateUpdatesCursorAndMatcherState() throws Exception {
        RecordingWalWriter wal = new RecordingWalWriter();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread loopThread = Thread.ofPlatform().start(loop);
        StandbySyncService service = new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());

        try {
            service.replicate(command(1L), 1_000_000L);
            service.markSnapshot(1L);
            awaitApplied(service, 1L);

            assertEquals(1, wal.appendCount);
            assertEquals(1, wal.forceCount);
            assertEquals(1L, matcher.lastSequence());
            assertEquals(new ReplicationCursor(1L, 1L, 1L, 1L), service.cursor());
        } finally {
            service.close();
            loop.stop();
            loopThread.join(5_000L);
        }
    }

    @Test
    @Tag("chaos")
    void replicateFailsAfterAsyncApplyLoopStops() throws Exception {
        RecordingWalWriter wal = new RecordingWalWriter();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(1);
        assertTrue(ring.offer(command(0L)));
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        StandbySyncService service = new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());

        try {
            service.replicate(command(1L), 1_000_000L);
            awaitApplyFailure(service);

            IOException error = assertThrows(IOException.class, () -> service.replicate(command(2L), 1_000_000L));

            assertEquals("standby apply loop failed", error.getMessage());
            assertEquals(1, wal.appendCount);
            assertEquals(1, wal.forceCount);
            assertEquals(new ReplicationCursor(1L, 1L, 0L, 0L), service.cursor());
        } finally {
            assertThrows(IOException.class, service::close);
        }
    }

    @Test
    void replicateAcknowledgesDurableSequenceBeforeApplyCompletes() throws Exception {
        RecordingWalWriter wal = new RecordingWalWriter();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        StandbySyncService service = new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());

        try {
            service.replicate(command(1L), TimeUnit.MILLISECONDS.toNanos(10));

            assertEquals(new ReplicationCursor(1L, 1L, 0L, 0L), service.cursor());
        } finally {
            service.close();
        }
    }

    @Test
    void replicateBatchForcesOnceAndAdvancesCursorToLastSequence() throws Exception {
        RecordingWalWriter wal = new RecordingWalWriter();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread loopThread = Thread.ofPlatform().start(loop);
        StandbySyncService service = new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());

        try {
            service.replicateBatch(List.of(command(1L), command(2L), command(3L)), TimeUnit.SECONDS.toNanos(1));
            awaitApplied(service, 3L);

            assertEquals(3, wal.appendCount);
            assertEquals(1, wal.forceCount);
            assertEquals(new ReplicationCursor(3L, 3L, 3L, 0L), service.cursor());
            assertEquals(3L, service.metricsSnapshot().lastReplicatedBatchSize());
            assertEquals(3L, service.metricsSnapshot().replicatedCommandsTotal());
            assertEquals(1L, service.metricsSnapshot().ackFlushCount());
        } finally {
            service.close();
            loop.stop();
            loopThread.join(5_000L);
        }
    }

    private static void awaitApplied(StandbySyncService service, long sequence) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline && service.cursor().lastAppliedSequence() < sequence) {
            Thread.sleep(10L);
        }
        assertEquals(sequence, service.cursor().lastAppliedSequence());
    }

    private static void awaitApplyFailure(StandbySyncService service) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            try {
                service.replicate(command(99L), 1L);
            } catch (IOException e) {
                if ("standby apply loop failed".equals(e.getMessage())) {
                    return;
                }
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("standby apply loop did not fail in time");
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 1000L + sequence, 2000L + sequence, 1, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 101L, 2L);
    }

    private static final class RecordingWalWriter implements WalWriter {
        private int appendCount;
        private int forceCount;

        @Override
        public void append(Command command) throws IOException {
            appendCount++;
        }

        @Override
        public void force() throws IOException {
            forceCount++;
        }

        @Override
        public void close() throws IOException {}
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }
}
