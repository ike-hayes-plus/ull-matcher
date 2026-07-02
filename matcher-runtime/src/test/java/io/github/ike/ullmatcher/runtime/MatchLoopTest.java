package io.github.ike.ullmatcher.runtime;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class MatchLoopTest {
    @Test
    void matchLoopRecordsFailureAndStops() throws Exception {
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        MatchEventHandler throwingHandler = new MatchEventHandler() {
            @Override
            public void onTrade(TradeEvent event) {}

            @Override
            public void onOrder(OrderEvent event) {
                throw new IllegalStateException("downstream failed");
            }
        };
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(
                new MatcherConfig(1, 1024, 4096, 4096, 100_000_000L, true), throwingHandler);
        AtomicReference<Throwable> captured = new AtomicReference<>();
        MatchLoop loop = new MatchLoop(ring, matcher, (command, error) -> captured.set(error));
        Thread thread = Thread.ofPlatform().name("matcher-test").start(loop);

        assertTrue(ring.offer(limit(1, 101, 11)));
        thread.join(1_000);

        assertFalse(loop.isRunning());
        assertEquals(MatchLoopState.FAILED, loop.state());
        assertNotNull(loop.failure());
        assertSame(loop.failure(), captured.get());
    }

    @Test
    void adaptiveIdleStrategyParksWhenIdleAndResetsOnTraffic() throws Exception {
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(
                ring,
                matcher,
                new MatchLoopConfig(new AdaptiveIdleStrategy(0, 0, 1_000L, 8_000L), 8),
                (command, error) -> {}
        );
        Thread thread = Thread.ofPlatform().name("matcher-idle-test").start(loop);

        awaitTrue(() -> loop.idleParkCount() > 0, Duration.ofSeconds(1));

        long parksBeforeTraffic = loop.idleParkCount();
        assertTrue(ring.offer(limit(1, 201, 21)));
        awaitTrue(() -> loop.processedCommandCount() == 1, Duration.ofSeconds(1));
        assertTrue(loop.snapshot().lastBatchSize() >= 1);
        assertTrue(loop.snapshot().maxBatchSizeObserved() >= 1);
        assertTrue(loop.idleParkCount() >= parksBeforeTraffic);

        assertTrue(ring.offer(Command.shutdown(2)));
        thread.join(1_000);
        assertEquals(MatchLoopState.STOPPED, loop.state());
    }

    @Test
    void drainAndStopStopsOnlyAfterQueuedCommandsAreConsumed() throws Exception {
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread thread = Thread.ofPlatform().name("matcher-drain-test").start(loop);

        assertTrue(ring.offer(limit(1, 301, 31)));
        assertTrue(ring.offer(limit(2, 302, 32)));
        loop.drainAndStop();

        thread.join(1_000);

        assertFalse(loop.isAcceptingCommands());
        assertEquals(MatchLoopState.STOPPED, loop.state());
        assertEquals(2, loop.processedCommandCount());
    }

    private static Command limit(long sequence, long orderId, long userId) {
        return Command.newOrder(sequence, orderId, userId, 1, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100, 5);
    }

    private static void awaitTrue(Check check, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(1L);
        }
        fail("condition not met before timeout");
    }

    @FunctionalInterface
    private interface Check {
        boolean ok() throws Exception;
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }
}
