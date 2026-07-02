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

import java.util.Locale;

/**
 * 纯撮合主链基准。
 * <p>
 * 该基准只保留：
 * <pre>{@code
 * Command -> UltraLowLatencyMatcher.onCommand(...)
 * }</pre>
 * 不包含网络、HTTP、WAL、HA、IPC、控制面和事件异步分发，用于和只测撮合/风控主链的项目做口径接近的比较。
 */
public final class CoreOnlyCrossingBenchmark {
    private static final int SYMBOL = 1;

    private CoreOnlyCrossingBenchmark() {
    }

    public static void main(String[] args) {
        Arguments parsed = Arguments.parse(args);
        warmUp(parsed);

        CountingHandler handler = new CountingHandler();
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(
                new MatcherConfig(
                        SYMBOL,
                        parsed.expectedPriceLevels(),
                        parsed.expectedLiveOrders(),
                        parsed.orderPoolSize(),
                        100_000_000L,
                        parsed.preventSelfTrade()
                ),
                handler
        );

        long sequence = 1L;
        for (int i = 0; i < parsed.restingOrders(); i++) {
            matcher.onCommand(Command.newOrder(
                    sequence++,
                    1_000_000L + i,
                    2L,
                    SYMBOL,
                    Side.SELL,
                    OrderType.LIMIT,
                    TimeInForce.GTC,
                    parsed.restingPrice() + (long) i * parsed.restingPriceStep(),
                    1L
            ));
        }

        Command[] crossingCommands = new Command[parsed.crossingOrders()];
        for (int i = 0; i < crossingCommands.length; i++) {
            crossingCommands[i] = Command.newOrder(
                    sequence++,
                    2_000_000L + i,
                    1L,
                    SYMBOL,
                    Side.BUY,
                    OrderType.LIMIT,
                    TimeInForce.IOC,
                    parsed.crossingPrice(),
                    1L
            );
        }

        int latencySampleInterval = parsed.latencySampleInterval();
        int latencyCapacity = latencySampleInterval == 0
                ? 0
                : (parsed.crossingOrders() + latencySampleInterval - 1) / latencySampleInterval;
        long[] latenciesNanos = new long[latencyCapacity];
        int latencySamples = 0;
        long started = System.nanoTime();
        if (latencySampleInterval == 0) {
            for (Command command : crossingCommands) {
                matcher.onCommand(command);
            }
        } else if (latencySampleInterval == 1) {
            for (int i = 0; i < parsed.crossingOrders(); i++) {
                long commandStarted = System.nanoTime();
                matcher.onCommand(crossingCommands[i]);
                latenciesNanos[latencySamples++] = System.nanoTime() - commandStarted;
            }
        } else {
            boolean powerOfTwoSampleInterval = (latencySampleInterval & (latencySampleInterval - 1)) == 0;
            int latencySampleMask = latencySampleInterval - 1;
            for (int i = 0; i < parsed.crossingOrders(); i++) {
                boolean sampleLatency = powerOfTwoSampleInterval
                        ? (i & latencySampleMask) == 0
                        : i % latencySampleInterval == 0;
                if (sampleLatency) {
                    long commandStarted = System.nanoTime();
                    matcher.onCommand(crossingCommands[i]);
                    latenciesNanos[latencySamples++] = System.nanoTime() - commandStarted;
                } else {
                    matcher.onCommand(crossingCommands[i]);
                }
            }
        }
        double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;
        boolean latencyMeasured = latencySamples > 0;

        System.out.println("{");
        System.out.println("  \"success\": true,");
        System.out.println("  \"scenario\": \"core_only_crossing_benchmark\",");
        BenchmarkSupport.printJsonMetadata();
        System.out.printf(Locale.ROOT, "  \"restingOrders\": %d,%n", parsed.restingOrders());
        System.out.printf(Locale.ROOT, "  \"crossingOrders\": %d,%n", parsed.crossingOrders());
        System.out.printf(Locale.ROOT, "  \"warmupOrders\": %d,%n", parsed.warmupOrders());
        System.out.printf(Locale.ROOT, "  \"latencySampleInterval\": %d,%n", latencySampleInterval);
        System.out.printf(Locale.ROOT, "  \"latencySamples\": %d,%n", latencySamples);
        System.out.printf(Locale.ROOT, "  \"latencyMeasured\": %s,%n", latencyMeasured);
        System.out.printf(Locale.ROOT, "  \"preventSelfTrade\": %s,%n", parsed.preventSelfTrade());
        System.out.printf(Locale.ROOT, "  \"elapsedSeconds\": %.6f,%n", elapsedSeconds);
        System.out.printf(Locale.ROOT, "  \"acceptedOrders\": %d,%n", parsed.crossingOrders());
        System.out.printf(Locale.ROOT, "  \"tradeEvents\": %d,%n", handler.tradeCount);
        System.out.printf(Locale.ROOT, "  \"orderEvents\": %d,%n", handler.orderEventCount);
        System.out.printf(Locale.ROOT, "  \"acceptedOrdersPerSecond\": %.2f,%n",
                BenchmarkSupport.perSecond(parsed.crossingOrders(), elapsedSeconds));
        System.out.printf(Locale.ROOT, "  \"tradeEventsPerSecond\": %.2f,%n",
                BenchmarkSupport.perSecond(handler.tradeCount, elapsedSeconds));
        if (latencyMeasured) {
            System.out.printf(Locale.ROOT, "  \"p50LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, latencySamples, 0.50));
            System.out.printf(Locale.ROOT, "  \"p90LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, latencySamples, 0.90));
            System.out.printf(Locale.ROOT, "  \"p95LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, latencySamples, 0.95));
            System.out.printf(Locale.ROOT, "  \"p99LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, latencySamples, 0.99));
            System.out.printf(Locale.ROOT, "  \"p999LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, latencySamples, 0.999));
            System.out.printf(Locale.ROOT, "  \"worstLatencyMicros\": %.2f%n", BenchmarkSupport.percentileMicros(latenciesNanos, latencySamples, 1.0));
        } else {
            System.out.println("  \"p50LatencyMicros\": null,");
            System.out.println("  \"p90LatencyMicros\": null,");
            System.out.println("  \"p95LatencyMicros\": null,");
            System.out.println("  \"p99LatencyMicros\": null,");
            System.out.println("  \"p999LatencyMicros\": null,");
            System.out.println("  \"worstLatencyMicros\": null");
        }
        System.out.println("}");
    }

    private static void warmUp(Arguments parsed) {
        CountingHandler handler = new CountingHandler();
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(
                new MatcherConfig(
                        SYMBOL,
                        parsed.expectedPriceLevels(),
                        parsed.expectedLiveOrders(),
                        parsed.orderPoolSize(),
                        100_000_000L,
                        parsed.preventSelfTrade()
                ),
                handler
        );
        long sequence = 1L;
        for (int i = 0; i < parsed.warmupOrders(); i++) {
            matcher.onCommand(Command.newOrder(
                    sequence++,
                    10_000_000L + i,
                    2L,
                    SYMBOL,
                    Side.SELL,
                    OrderType.LIMIT,
                    TimeInForce.GTC,
                    parsed.restingPrice(),
                    1L
            ));
        }
        Command[] commands = new Command[parsed.warmupOrders()];
        for (int i = 0; i < commands.length; i++) {
            commands[i] = Command.newOrder(
                    sequence++,
                    20_000_000L + i,
                    1L,
                    SYMBOL,
                    Side.BUY,
                    OrderType.LIMIT,
                    TimeInForce.IOC,
                    parsed.crossingPrice(),
                    1L
            );
        }
        for (Command command : commands) {
            matcher.onCommand(command);
        }
    }

    private record Arguments(int restingOrders,
                             int crossingOrders,
                             int warmupOrders,
                             int latencySampleInterval,
                             int expectedPriceLevels,
                             int expectedLiveOrders,
                             int orderPoolSize,
                             long restingPrice,
                             long restingPriceStep,
                             long crossingPrice,
                             boolean preventSelfTrade) {
        private static Arguments parse(String[] args) {
            int restingOrders = 2_048;
            int crossingOrders = 1_000_000;
            int warmupOrders = 200_000;
            int latencySampleInterval = 1;
            int expectedPriceLevels = 4_096;
            int expectedLiveOrders = 1 << 16;
            int orderPoolSize = 1 << 17;
            long restingPrice = 100L;
            long restingPriceStep = 1L;
            long crossingPrice = 101L;
            boolean preventSelfTrade = true;
            for (String arg : args) {
                if (arg.startsWith("--restingOrders=")) {
                    restingOrders = Integer.parseInt(arg.substring("--restingOrders=".length()));
                } else if (arg.startsWith("--crossingOrders=")) {
                    crossingOrders = Integer.parseInt(arg.substring("--crossingOrders=".length()));
                } else if (arg.startsWith("--warmupOrders=")) {
                    warmupOrders = Integer.parseInt(arg.substring("--warmupOrders=".length()));
                } else if (arg.startsWith("--latencySampleInterval=")) {
                    latencySampleInterval = Integer.parseInt(arg.substring("--latencySampleInterval=".length()));
                } else if (arg.startsWith("--expectedPriceLevels=")) {
                    expectedPriceLevels = Integer.parseInt(arg.substring("--expectedPriceLevels=".length()));
                } else if (arg.startsWith("--expectedLiveOrders=")) {
                    expectedLiveOrders = Integer.parseInt(arg.substring("--expectedLiveOrders=".length()));
                } else if (arg.startsWith("--orderPoolSize=")) {
                    orderPoolSize = Integer.parseInt(arg.substring("--orderPoolSize=".length()));
                } else if (arg.startsWith("--restingPrice=")) {
                    restingPrice = Long.parseLong(arg.substring("--restingPrice=".length()));
                } else if (arg.startsWith("--restingPriceStep=")) {
                    restingPriceStep = Long.parseLong(arg.substring("--restingPriceStep=".length()));
                } else if (arg.startsWith("--crossingPrice=")) {
                    crossingPrice = Long.parseLong(arg.substring("--crossingPrice=".length()));
                } else if (arg.startsWith("--preventSelfTrade=")) {
                    preventSelfTrade = Boolean.parseBoolean(arg.substring("--preventSelfTrade=".length()));
                } else {
                    throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }
            if (latencySampleInterval < 0) {
                throw new IllegalArgumentException("--latencySampleInterval must be >= 0");
            }
            return new Arguments(
                    restingOrders,
                    crossingOrders,
                    warmupOrders,
                    latencySampleInterval,
                    expectedPriceLevels,
                    expectedLiveOrders,
                    orderPoolSize,
                    restingPrice,
                    restingPriceStep,
                    crossingPrice,
                    preventSelfTrade
            );
        }
    }

    private static final class CountingHandler implements MatchEventHandler {
        private long tradeCount;
        private long orderEventCount;

        @Override
        public void onTrade(TradeEvent event) {
            tradeCount++;
        }

        @Override
        public void onOrder(OrderEvent event) {
            orderEventCount++;
        }
    }
}
