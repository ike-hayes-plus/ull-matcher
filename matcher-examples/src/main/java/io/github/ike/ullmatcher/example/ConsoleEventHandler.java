package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.TradeEvent;

import java.io.PrintStream;

/**
 * 将撮合事件打印到输出流的示例事件处理器。
 */
final class ConsoleEventHandler implements MatchEventHandler {
    /** 示例输出目标流。 */
    private final PrintStream out;

    /**
     * 创建控制台事件处理器。
     *
     * @param out 目标输出流
     */
    ConsoleEventHandler(PrintStream out) {
        this.out = out;
    }

    /**
     * 打印成交事件。
     *
     * @param event 成交事件
     */
    @Override
    public void onTrade(TradeEvent event) {
        out.println(event);
    }

    /**
     * 打印订单事件。
     *
     * @param event 订单事件
     */
    @Override
    public void onOrder(OrderEvent event) {
        out.println(event);
    }
}
