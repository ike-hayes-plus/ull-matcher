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

import java.util.ArrayList;
import java.util.List;
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
        CountingHandler handler = new CountingHandler();
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(
                new MatcherConfig(
                        SYMBOL,
                        parsed.expectedPriceLevels(),
                        parsed.expectedLiveOrders(),
                        parsed.orderPoolSize(),
                        100_000_000L,
                        true
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

        ArrayList<Long> latenciesNanos = new ArrayList<>(parsed.crossingOrders());
        long started = System.nanoTime();
        for (int i = 0; i < parsed.crossingOrders(); i++) {
            long commandStarted = System.nanoTime();
            matcher.onCommand(Command.newOrder(
                    sequence++,
                    2_000_000L + i,
                    1L,
                    SYMBOL,
                    Side.BUY,
                    OrderType.LIMIT,
                    TimeInForce.IOC,
                    parsed.crossingPrice(),
                    1L
            ));
            latenciesNanos.add(System.nanoTime() - commandStarted);
        }
        double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;

        System.out.println("{");
        System.out.println("  \"success\": true,");
        System.out.println("  \"scenario\": \"core_only_crossing_benchmark\",");
        System.out.printf(Locale.ROOT, "  \"restingOrders\": %d,%n", parsed.restingOrders());
        System.out.printf(Locale.ROOT, "  \"crossingOrders\": %d,%n", parsed.crossingOrders());
        System.out.printf(Locale.ROOT, "  \"elapsedSeconds\": %.6f,%n", elapsedSeconds);
        System.out.printf(Locale.ROOT, "  \"acceptedOrders\": %d,%n", parsed.crossingOrders());
        System.out.printf(Locale.ROOT, "  \"tradeEvents\": %d,%n", handler.tradeCount);
        System.out.printf(Locale.ROOT, "  \"orderEvents\": %d,%n", handler.orderEventCount);
        System.out.printf(Locale.ROOT, "  \"acceptedOrdersPerSecond\": %.2f,%n",
                BenchmarkSupport.perSecond(parsed.crossingOrders(), elapsedSeconds));
        System.out.printf(Locale.ROOT, "  \"tradeEventsPerSecond\": %.2f,%n",
                BenchmarkSupport.perSecond(handler.tradeCount, elapsedSeconds));
        System.out.printf(Locale.ROOT, "  \"p50LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, 0.50));
        System.out.printf(Locale.ROOT, "  \"p90LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, 0.90));
        System.out.printf(Locale.ROOT, "  \"p95LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, 0.95));
        System.out.printf(Locale.ROOT, "  \"p99LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, 0.99));
        System.out.printf(Locale.ROOT, "  \"p999LatencyMicros\": %.2f,%n", BenchmarkSupport.percentileMicros(latenciesNanos, 0.999));
        System.out.printf(Locale.ROOT, "  \"worstLatencyMicros\": %.2f%n", BenchmarkSupport.percentileMicros(latenciesNanos, 1.0));
        System.out.println("}");
    }

    private record Arguments(int restingOrders,
                             int crossingOrders,
                             int expectedPriceLevels,
                             int expectedLiveOrders,
                             int orderPoolSize,
                             long restingPrice,
                             long restingPriceStep,
                             long crossingPrice) {
        private static Arguments parse(String[] args) {
            int restingOrders = 2_048;
            int crossingOrders = 1_000_000;
            int expectedPriceLevels = 4_096;
            int expectedLiveOrders = 1 << 16;
            int orderPoolSize = 1 << 17;
            long restingPrice = 100L;
            long restingPriceStep = 1L;
            long crossingPrice = 101L;
            for (String arg : args) {
                if (arg.startsWith("--restingOrders=")) {
                    restingOrders = Integer.parseInt(arg.substring("--restingOrders=".length()));
                } else if (arg.startsWith("--crossingOrders=")) {
                    crossingOrders = Integer.parseInt(arg.substring("--crossingOrders=".length()));
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
                } else {
                    throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }
            return new Arguments(
                    restingOrders,
                    crossingOrders,
                    expectedPriceLevels,
                    expectedLiveOrders,
                    orderPoolSize,
                    restingPrice,
                    restingPriceStep,
                    crossingPrice
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
