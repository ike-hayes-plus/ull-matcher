package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.hft.AsyncEventDispatcher;
import io.github.ike.ullmatcher.hft.JournaledMatcherGateway;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.queue.MpscArrayQueue;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.AdaptiveIdleStrategy;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.storage.wal.SegmentedMmapWal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单进程、单交易对 crossing benchmark。
 * <p>
 * 该基准绕开 HTTP、HA 和控制面，只保留：
 * <pre>{@code
 * gateway -> WAL -> ring -> match loop -> event dispatcher
 * }</pre>
 * 用于测量单 jar 引用下的接单能力与撮合能力。
 */
public final class EmbeddedCrossingBenchmark {
    private static final int SYMBOL = 1;

    private EmbeddedCrossingBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Arguments parsed = Arguments.parse(args);
        Path walDir = parsed.walDir();
        Files.createDirectories(walDir);

        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(parsed.ringCapacity());
        AsyncEventDispatcher dispatcher = new AsyncEventDispatcher(parsed.eventQueueCapacity());
        CountingHandler downstream = new CountingHandler();
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(
                new MatcherConfig(SYMBOL, parsed.expectedPriceLevels(), parsed.expectedLiveOrders(), parsed.orderPoolSize(), 100_000_000L, true),
                dispatcher
        );
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread matcherThread = Thread.ofPlatform().name("embed-bench-matcher").start(loop);
        Thread eventThread = Thread.ofPlatform().name("embed-bench-events").start(() -> runEventDrain(loop, dispatcher, downstream));

        try (SegmentedMmapWal wal = new SegmentedMmapWal(walDir, "embed-bench", parsed.walSegmentBytes())) {
            JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                    wal,
                    ring,
                    10_000,
                    TimeUnit.SECONDS.toNanos(5),
                    loop::isAcceptingCommands,
                    parsed.durabilityMode(),
                    parsed.forceBatchSize(),
                    parsed.forceMaxDelayMicros()
            );

            long sequence = 1L;
            for (int i = 0; i < parsed.restingOrders(); i++) {
                gateway.submit(Command.newOrder(sequence++, 1_000_000L + i, 2L, SYMBOL, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L));
            }
            waitForProcessed(loop, parsed.restingOrders(), 30L);

            long acceptedBefore = gateway.acceptedCount();
            long processedBefore = loop.processedCommandCount();
            long tradesBefore = downstream.tradeCount.get();
            long ordersBefore = downstream.orderEventCount.get();
            MpscArrayQueue<CrossingRequest> ingressQueue = new MpscArrayQueue<>(parsed.ingressQueueCapacity());
            AtomicLong offered = new AtomicLong();
            AtomicLong accepted = new AtomicLong();
            AtomicLong rejected = new AtomicLong();
            CountDownLatch producersDone = new CountDownLatch(parsed.concurrency());

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService submitters = Executors.newFixedThreadPool(parsed.concurrency(), Thread.ofPlatform().name("embed-bench-submit-", 0).factory());
            long startOrderId = 2_000_000L;
            long benchmarkStart = System.nanoTime();
            final long firstCrossingSequence = sequence;
            Thread publisher = Thread.ofPlatform().name("embed-bench-publisher").start(() -> {
                ArrayList<Command> batch = new ArrayList<>(parsed.publisherBatchSize());
                long publisherSequence = firstCrossingSequence;
                try {
                    while (producersDone.getCount() > 0 || ingressQueue.size() > 0) {
                        batch.clear();
                        CrossingRequest first = ingressQueue.poll();
                        if (first == null) {
                            Thread.onSpinWait();
                            continue;
                        }
                        batch.add(first.toCommand(publisherSequence++));
                        ArrayList<CrossingRequest> pending = new ArrayList<>(parsed.publisherBatchSize() - 1);
                        ingressQueue.drainTo(pending, parsed.publisherBatchSize() - 1);
                        for (CrossingRequest request : pending) {
                            batch.add(request.toCommand(publisherSequence++));
                        }
                        SubmitResult result = gateway.trySubmitBatch(batch, TimeUnit.SECONDS.toNanos(5));
                        if (result == SubmitResult.ACCEPTED) {
                            accepted.addAndGet(batch.size());
                        } else {
                            rejected.addAndGet(batch.size());
                            for (Command command : batch) {
                                command.release();
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            for (int worker = 0; worker < parsed.concurrency(); worker++) {
                final int workerIndex = worker;
                submitters.submit(() -> {
                    try {
                        start.await();
                        for (int i = workerIndex; i < parsed.crossingOrders(); i += parsed.concurrency()) {
                            CrossingRequest request = new CrossingRequest(startOrderId + i, 1L, 101L, 1L);
                            while (!ingressQueue.offer(request)) {
                                Thread.onSpinWait();
                            }
                            offered.incrementAndGet();
                        }
                    } finally {
                        producersDone.countDown();
                    }
                    return null;
                });
            }
            start.countDown();
            submitters.shutdown();
            if (!submitters.awaitTermination(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("submit benchmark did not finish in time");
            }
            publisher.join();
            waitForProcessed(loop, parsed.restingOrders() + accepted.get(), 60L);
            long benchmarkElapsedNanos = System.nanoTime() - benchmarkStart;
            long acceptedAfterWork = gateway.acceptedCount();

            gateway.submit(Command.shutdown(sequence + accepted.get()));
            matcherThread.join();
            eventThread.join();

            long acceptedDelta = acceptedAfterWork - acceptedBefore;
            long processedDelta = loop.processedCommandCount() - processedBefore;
            long tradesDelta = downstream.tradeCount.get() - tradesBefore;
            long ordersDelta = downstream.orderEventCount.get() - ordersBefore;
            double seconds = benchmarkElapsedNanos / 1_000_000_000.0;

            System.out.println("{");
            System.out.printf(Locale.ROOT, "  \"success\": %s,%n", accepted.get() == parsed.crossingOrders() && rejected.get() == 0L);
            System.out.println("  \"scenario\": \"embedded_crossing_benchmark\",");
            System.out.printf(Locale.ROOT, "  \"restingOrders\": %d,%n", parsed.restingOrders());
            System.out.printf(Locale.ROOT, "  \"crossingOrders\": %d,%n", parsed.crossingOrders());
            System.out.printf(Locale.ROOT, "  \"concurrency\": %d,%n", parsed.concurrency());
            System.out.printf(Locale.ROOT, "  \"elapsedSeconds\": %.6f,%n", seconds);
            System.out.printf(Locale.ROOT, "  \"acceptedOrders\": %d,%n", acceptedDelta);
            System.out.printf(Locale.ROOT, "  \"offeredOrders\": %d,%n", offered.get());
            System.out.printf(Locale.ROOT, "  \"processedCommands\": %d,%n", processedDelta);
            System.out.printf(Locale.ROOT, "  \"tradeEvents\": %d,%n", tradesDelta);
            System.out.printf(Locale.ROOT, "  \"orderEvents\": %d,%n", ordersDelta);
            System.out.printf(Locale.ROOT, "  \"acceptedOrdersPerSecond\": %.2f,%n", seconds == 0.0 ? 0.0 : acceptedDelta / seconds);
            System.out.printf(Locale.ROOT, "  \"processedCommandsPerSecond\": %.2f,%n", seconds == 0.0 ? 0.0 : processedDelta / seconds);
            System.out.printf(Locale.ROOT, "  \"tradeEventsPerSecond\": %.2f,%n", seconds == 0.0 ? 0.0 : tradesDelta / seconds);
            System.out.printf(Locale.ROOT, "  \"orderEventsPerSecond\": %.2f,%n", seconds == 0.0 ? 0.0 : ordersDelta / seconds);
            System.out.printf(Locale.ROOT, "  \"rejectedOrders\": %d%n", rejected.get());
            System.out.println("}");
        }
    }

    private static void runEventDrain(MatchLoop loop, AsyncEventDispatcher dispatcher, MatchEventHandler downstream) {
        AdaptiveIdleStrategy idle = AdaptiveIdleStrategy.defaults();
        long consecutiveIdle = 0L;
        while (loop.isRunning() || dispatcher.size() > 0) {
            if (dispatcher.drainTo(downstream, 4_096) == 0) {
                idle.idle(++consecutiveIdle);
            } else {
                consecutiveIdle = 0L;
                idle.reset();
            }
        }
    }

    private static void waitForProcessed(MatchLoop loop, long expected, long timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (loop.processedCommandCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        if (loop.processedCommandCount() < expected) {
            throw new IllegalStateException("match loop did not process expected commands");
        }
    }

    private record Arguments(int restingOrders,
                             int crossingOrders,
                             int concurrency,
                             int ingressQueueCapacity,
                             int publisherBatchSize,
                             int ringCapacity,
                             int eventQueueCapacity,
                             int expectedPriceLevels,
                             int expectedLiveOrders,
                             int orderPoolSize,
                             long walSegmentBytes,
                             WalDurabilityMode durabilityMode,
                             int forceBatchSize,
                             long forceMaxDelayMicros,
                             Path walDir) {
        private static Arguments parse(String[] args) {
            int restingOrders = 2_048;
            int crossingOrders = 2_048;
            int concurrency = 24;
            int ingressQueueCapacity = 1 << 16;
            int publisherBatchSize = 256;
            int ringCapacity = 1 << 16;
            int eventQueueCapacity = 1 << 16;
            int expectedPriceLevels = 4_096;
            int expectedLiveOrders = 65_536;
            int orderPoolSize = 131_072;
            long walSegmentBytes = 64L * 1024L * 1024L;
            WalDurabilityMode durabilityMode = WalDurabilityMode.SYNC_PER_BATCH;
            int forceBatchSize = 32;
            long forceMaxDelayMicros = 500L;
            Path walDir = Path.of("target", "embed-bench-wal");
            List<String> tokens = new ArrayList<>(List.of(args));
            for (int i = 0; i < tokens.size(); i += 2) {
                String key = tokens.get(i);
                String value = i + 1 < tokens.size() ? tokens.get(i + 1) : "";
                switch (key) {
                    case "--resting-orders" -> restingOrders = Integer.parseInt(value);
                    case "--crossing-orders" -> crossingOrders = Integer.parseInt(value);
                    case "--concurrency" -> concurrency = Integer.parseInt(value);
                    case "--ingress-queue-capacity" -> ingressQueueCapacity = Integer.parseInt(value);
                    case "--publisher-batch-size" -> publisherBatchSize = Integer.parseInt(value);
                    case "--ring-capacity" -> ringCapacity = Integer.parseInt(value);
                    case "--event-queue-capacity" -> eventQueueCapacity = Integer.parseInt(value);
                    case "--expected-price-levels" -> expectedPriceLevels = Integer.parseInt(value);
                    case "--expected-live-orders" -> expectedLiveOrders = Integer.parseInt(value);
                    case "--order-pool-size" -> orderPoolSize = Integer.parseInt(value);
                    case "--wal-segment-bytes" -> walSegmentBytes = Long.parseLong(value);
                    case "--durability-mode" -> durabilityMode = WalDurabilityMode.valueOf(value);
                    case "--force-batch-size" -> forceBatchSize = Integer.parseInt(value);
                    case "--force-max-delay-micros" -> forceMaxDelayMicros = Long.parseLong(value);
                    case "--wal-dir" -> walDir = Path.of(value);
                    default -> throw new IllegalArgumentException("unknown argument: " + key);
                }
            }
            return new Arguments(
                    restingOrders,
                    crossingOrders,
                    concurrency,
                    ingressQueueCapacity,
                    publisherBatchSize,
                    ringCapacity,
                    eventQueueCapacity,
                    expectedPriceLevels,
                    expectedLiveOrders,
                    orderPoolSize,
                    walSegmentBytes,
                    durabilityMode,
                    forceBatchSize,
                    forceMaxDelayMicros,
                    walDir
            );
        }
    }

    private static final class CountingHandler implements MatchEventHandler {
        private final AtomicLong tradeCount = new AtomicLong();
        private final AtomicLong orderEventCount = new AtomicLong();

        @Override
        public void onTrade(TradeEvent event) {
            tradeCount.incrementAndGet();
        }

        @Override
        public void onOrder(OrderEvent event) {
            orderEventCount.incrementAndGet();
        }
    }

    private record CrossingRequest(long orderId, long userId, long price, long quantity) {
        private Command toCommand(long sequence) {
            return Command.newOrder(sequence, orderId, userId, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, price, quantity);
        }
    }
}
