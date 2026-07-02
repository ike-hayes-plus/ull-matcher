package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.api.*;
import io.github.ike.ullmatcher.core.*;

/** 运行方式：{@code java -cp ... io.github.ike.ullmatcher.example.ExampleMain} */
public final class ExampleMain {
    /**
     * 工具类。
     */
    private ExampleMain() {}

    /**
     * 运行最小内存撮合示例。
     *
     */
    public static void main(String[] args) {
        MatchEventHandler handler = new ConsoleEventHandler(System.out);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), handler);
        long scale = 100_000_000L;

        matcher.onCommand(Command.newOrder(1, 1001, 10, 1, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 98 * scale, scale));
        matcher.onCommand(Command.newOrder(2, 1002, 11, 1, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 99 * scale, 2 * scale));
        matcher.onCommand(Command.newOrder(3, 2001, 20, 1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100 * scale, 2 * scale));
    }
}
