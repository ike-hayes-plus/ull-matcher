package io.github.ike.ullmatcher.benchmark;

import io.github.ike.ullmatcher.api.*;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * onCommand 热路径的 JMH 微基准。
 * <p>
 * 运行方式：{@code mvn -Pbenchmark -DskipTests package && java -jar target/ull-matcher-*-benchmarks.jar}
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseZGC", "-XX:+AlwaysPreTouch", "-Xms2g", "-Xmx2g"})
@State(Scope.Thread)
public class MatcherThroughputBenchmark {
    /** 基准测试交易对编号。 */
    private static final int SYMBOL = 1001;

    /** 每次迭代都会重建的撮合器实例。 */
    private UltraLowLatencyMatcher matcher;

    /** 生成基准测试订单使用的命令序列号。 */
    private long seq;

    /** 生成基准测试订单使用的订单编号序列。 */
    private long orderId;

    /**
     * 构建撮合器，并为吞吐量基准预置卖盘流动性。
     *
     * @param bh 事件处理器使用的黑洞对象
     */
    @Setup(Level.Iteration)
    public void setup(Blackhole bh) {
        matcher = new UltraLowLatencyMatcher(
                new MatcherConfig(SYMBOL, 1 << 16, 1 << 20, 1 << 20, 100_000_000L, false),
                new MatchEventHandler() {
                    @Override public void onTrade(TradeEvent event) { bh.consume(event.tradeId); }
                    @Override public void onOrder(OrderEvent event) { bh.consume(event.orderId); }
                });
        seq = 1;
        orderId = 1;
        // 预置卖盘流动性，基准测试随后发送可成交的 IOC 买单。
        for (int i = 0; i < 200_000; i++) {
            matcher.onCommand(Command.newOrder(seq++, orderId++, 10_000_000L + i, SYMBOL,
                    Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100_000 + (i % 16), 1_000));
        }
    }

    /**
     * 测试立即可成交 IOC 买单的吞吐量。
     */
    @Benchmark
    public void marketableIocBuy() {
        matcher.onCommand(Command.newOrder(seq++, orderId++, 20_000_000L + orderId, SYMBOL,
                Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 100_015, 1));
    }

    /**
     * 测试会挂入订单簿的只挂单卖单吞吐量。
     */
    @Benchmark
    public void restingPostOnlySell() {
        matcher.onCommand(Command.newOrder(seq++, orderId++, 30_000_000L + orderId, SYMBOL,
                Side.SELL, OrderType.LIMIT, TimeInForce.POST_ONLY, 101_000 + (orderId & 127), 1));
    }
}
