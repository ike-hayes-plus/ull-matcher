package io.github.ike.ullmatcher.benchmark;

import io.github.ike.ullmatcher.api.*;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * 高频交易延迟基准：测试单线程撮合器的热路径耗时。
 * <p>
 * 注意：JMH 输出是微基准结果，不等于完整交易系统延迟。
 * 完整链路还包含网关、风控、冻结、WAL、行情、清结算和网络。
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
@State(Scope.Thread)
public class MatcherHftLatencyBenchmark {
    /** 基准测试线程使用的撮合器实例。 */
    private UltraLowLatencyMatcher matcher;

    /** 生成基准测试订单使用的命令序列号。 */
    private long seq;

    /** 生成基准测试订单使用的订单编号序列。 */
    private long orderId;

    /**
     * 构建撮合器并预加载卖侧流动性。
     */
    @Setup
    public void setup() {
        matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        seq = 1;
        orderId = 1;
        // 预置卖盘，让后续买单能够立即吃单成交。
        for (int i = 0; i < 100_000; i++) {
            matcher.onCommand(Command.newOrder(seq++, orderId++, 1000 + i, 1, Side.SELL,
                    OrderType.LIMIT, TimeInForce.GTC, 100_000_000L + i, 1_000_000L));
        }
    }

    /**
     * 采样会立即产生成交的买单延迟。
     */
    @Benchmark
    public void matchOneFill() {
        matcher.onCommand(Command.newOrder(seq++, orderId++, 999_999, 1, Side.BUY,
                OrderType.LIMIT, TimeInForce.IOC, 200_000_000L, 1_000L));
    }

    /**
     * 有意丢弃全部基准测试事件的事件处理器。
     */
    private static final class NoopHandler implements MatchEventHandler {
        /**
         * 丢弃成交事件。
         *
         * @param event 成交事件
         */
        @Override
        public void onTrade(TradeEvent event) {}

        /**
         * 丢弃订单事件。
         *
         * @param event 订单事件
         */
        @Override
        public void onOrder(OrderEvent event) {}
    }
}
