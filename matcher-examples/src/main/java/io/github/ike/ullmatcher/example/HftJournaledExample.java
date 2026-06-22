package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.api.*;
import io.github.ike.ullmatcher.core.*;
import io.github.ike.ullmatcher.hft.AsyncEventDispatcher;
import io.github.ike.ullmatcher.hft.JournaledMatcherGateway;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.AdaptiveIdleStrategy;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.storage.wal.SegmentedMmapWal;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * 高频交易风格启动示例：WAL -> 单生产者单消费者环形缓冲区 -> 固定撮合线程。
 */
public final class HftJournaledExample {
    /**
     * 工具类。
     */
    private HftJournaledExample() {}

    /**
     * 运行带日志的撮合器示例。
     *
     * @throws Exception WAL 或撮合线程操作失败时抛出
     */
    public static void main(String[] args) throws Exception {
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(1 << 16);
        MatchEventHandler downstream = new ConsoleEventHandler(System.out);
        AsyncEventDispatcher events = new AsyncEventDispatcher(1 << 16);

        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), events);
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread matcherThread = Thread.ofPlatform().name("matcher-symbol-1").start(loop);
        Thread eventThread = Thread.ofPlatform().name("matcher-events-symbol-1").start(() -> {
            AdaptiveIdleStrategy idle = AdaptiveIdleStrategy.defaults();
            long consecutiveIdleCount = 0L;
            while (loop.isRunning() || events.size() > 0) {
                if (events.drainTo(downstream, 1024) == 0) {
                    idle.idle(++consecutiveIdleCount);
                } else {
                    consecutiveIdleCount = 0L;
                    idle.reset();
                }
            }
        });

        try (SegmentedMmapWal wal = new SegmentedMmapWal(Path.of("target/wal"), "symbol-1", 64L * 1024 * 1024)) {
            JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                    wal, ring, 10_000, TimeUnit.SECONDS.toNanos(1), loop::isAcceptingCommands);
            gateway.submit(Command.newOrder(1, 101, 1, 1, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100_000_000L, 10_000L));
            gateway.submit(Command.newOrder(2, 102, 2, 1, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 100_000_000L, 5_000L));
            gateway.flushWal();
            gateway.submit(Command.shutdown(3));
        }

        matcherThread.join();
        eventThread.join();
    }
}
