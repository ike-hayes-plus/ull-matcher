package io.github.ike.ullmatcher.core;

import io.github.ike.ullmatcher.api.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 撮合器确定性行为单元测试。
 */
class UltraLowLatencyMatcherTest {
    /** 所有单元测试使用的交易对编号。 */
    private static final int SYMBOL = 1001;

    /**
     * 创建测试容量配置的撮合器。
     *
     * @param h 记录事件处理器
     * @return 待测试撮合器
     */
    private static UltraLowLatencyMatcher matcher(RecordingHandler h) {
        return new UltraLowLatencyMatcher(new MatcherConfig(SYMBOL, 1024, 4096, 4096, 100_000_000L, true), h);
    }

    /**
     * 为测试交易对创建 GTC 限价订单命令。
     *
     * @param seq 命令序列号
     * @param orderId 订单编号
     * @param userId 用户编号
     * @param side 订单方向
     * @param price 限价
     * @param qty 订单数量
     * @return 新订单命令
     */
    private static Command limit(long seq, long orderId, long userId, Side side, long price, long qty) {
        return Command.newOrder(seq, orderId, userId, SYMBOL, side, OrderType.LIMIT, TimeInForce.GTC, price, qty);
    }

    /**
     * 验证价格时间优先和部分成交记账的确定性。
     */
    @Test
    void priceTimePriorityAndPartialFillAreDeterministic() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(limit(2, 102, 12, Side.SELL, 100, 7));
        m.onCommand(limit(3, 103, 13, Side.SELL, 101, 9));
        m.onCommand(limit(4, 201, 21, Side.BUY, 100, 8));

        assertEquals(2, h.trades.size());
        assertEquals(101, h.trades.get(0).sellOrderId());
        assertEquals(5, h.trades.get(0).quantity());
        assertEquals(102, h.trades.get(1).sellOrderId());
        assertEquals(3, h.trades.get(1).quantity());
        assertEquals(2, m.liveOrderCount(), "sell 102 has 4 remaining and sell 103 is untouched");
    }

    /**
     * 验证撤单会在后续撮合前移除挂单。
     */
    @Test
    void cancelRemovesRestingOrder() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(Command.cancel(2, 101, SYMBOL));
        m.onCommand(limit(3, 201, 21, Side.BUY, 100, 5));

        assertEquals(0, h.trades.size());
        assertEquals(1, m.liveOrderCount(), "buy order rests because cancelled sell order is gone");
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 101 && o.status().equals("CANCELLED")));
    }

    /**
     * 验证 IOC 会撤销未成交剩余数量。
     */
    @Test
    void iocCancelsUnfilledRemainder() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(2, 201, 21, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 100, 9));

        assertEquals(1, h.trades.size());
        assertEquals(5, h.trades.getFirst().quantity());
        assertEquals(0, m.liveOrderCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("PARTIALLY_FILLED") && o.remaining() == 4));
    }

    /**
     * 验证部分成交会同步扣减价格档数量，后续 FOK 不会高估流动性。
     */
    @Test
    void fokUsesRemainingPriceLevelQuantityAfterPartialFill() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(2, 201, 21, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 100, 3));
        m.onCommand(Command.newOrder(3, 202, 22, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, 100, 3));

        assertEquals(1, h.trades.size());
        assertEquals(1, m.liveOrderCount());
        assertEquals(2, h.orders.stream()
                .filter(o -> o.orderId() == 101 && o.status().equals("PARTIALLY_FILLED"))
                .findFirst()
                .orElseThrow()
                .remaining());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 202 && o.status().equals("CANCELLED")
                && o.rejectReason().equals("FOK_NOT_FILLABLE")));
    }

    /**
     * 验证 FOK 在防自成交路径上不会先部分成交再取消剩余。
     */
    @Test
    void fokRejectsSelfTradePathWithoutPartialMutation() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 12, Side.SELL, 100, 5));
        m.onCommand(limit(2, 102, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(3, 201, 11, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, 100, 6));

        assertEquals(0, h.trades.size());
        assertEquals(2, m.liveOrderCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("REJECTED")
                && o.rejectReason().equals("SELF_TRADE_PREVENTED")));
    }

    /**
     * 验证防自成交 fast path 不会因为可达数量之后的同用户挂单误拒来单。
     */
    @Test
    void selfTradeFastPathAllowsFillBeforeLaterSelfOrder() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 12, Side.SELL, 100, 5));
        m.onCommand(limit(2, 102, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(3, 201, 11, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 100, 5));

        assertEquals(1, h.trades.size());
        assertEquals(101, h.trades.getFirst().sellOrderId());
        assertEquals(5, h.trades.getFirst().quantity());
        assertEquals(1, m.liveOrderCount(), "later same-user resting order must remain untouched");
        assertFalse(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("REJECTED")));
    }

    /**
     * 验证 FOK fast path 只按实际填满路径判断，不会扫描到填满之后的同用户挂单后误取消。
     */
    @Test
    void fokFastPathAllowsFullFillBeforeLaterSelfOrder() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 12, Side.SELL, 100, 5));
        m.onCommand(limit(2, 102, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(3, 201, 11, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, 100, 5));

        assertEquals(1, h.trades.size());
        assertEquals(101, h.trades.getFirst().sellOrderId());
        assertEquals(5, h.trades.getFirst().quantity());
        assertEquals(1, m.liveOrderCount(), "FOK should fill only the first resting order");
        assertFalse(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("CANCELLED")
                && o.rejectReason().equals("FOK_NOT_FILLABLE")));
        assertFalse(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("REJECTED")
                && o.rejectReason().equals("SELF_TRADE_PREVENTED")));
    }

    /**
     * 验证不穿透盘口的同用户 IOC 不应走防自成交拒绝路径。
     */
    @Test
    void nonCrossingIocIsCancelledInsteadOfSelfTradeRejected() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(2, 201, 11, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 99, 5));

        assertEquals(0, h.trades.size());
        assertEquals(1, m.liveOrderCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("CANCELLED")
                && o.rejectReason().equals("NONE")));
        assertFalse(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("REJECTED")
                && o.rejectReason().equals("SELF_TRADE_PREVENTED")));
    }

    /**
     * 验证 FOK 预检查失败不会修改订单簿。
     */
    @Test
    void fokDoesNotMutateBookWhenNotFullyFillable() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(2, 201, 21, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.FOK, 100, 9));

        assertEquals(0, h.trades.size());
        assertEquals(1, m.liveOrderCount(), "resting sell order must remain after failed FOK");
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("CANCELLED") && o.remaining() == 9));
    }

    /**
     * 验证只挂单订单在会穿透时被拒绝。
     */
    @Test
    void postOnlyRejectsWhenItWouldTake() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(2, 201, 21, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.POST_ONLY, 100, 1));

        assertEquals(0, h.trades.size());
        assertEquals(1, m.liveOrderCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("REJECTED")
                && o.rejectReason().equals("POST_ONLY_WOULD_TAKE")));
    }

    /**
     * 验证防自成交逻辑会拒绝来单。
     */
    @Test
    void selfTradePreventionRejectsTaker() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(limit(2, 201, 11, Side.BUY, 100, 5));

        assertEquals(0, h.trades.size());
        assertEquals(1, m.liveOrderCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("REJECTED")
                && o.rejectReason().equals("SELF_TRADE_PREVENTED")));
    }

    /**
     * 验证重复命令序列会被拒绝且不修改订单簿。
     */
    @Test
    void duplicateSequenceIsRejectedAndDoesNotMutateBook() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(limit(1, 102, 12, Side.SELL, 99, 5));

        assertEquals(1, m.liveOrderCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 102 && o.status().equals("REJECTED")
                && o.rejectReason().equals("DUPLICATE_SEQUENCE")));
    }

    /**
     * 验证非法订单编号会被拒绝，不会让订单索引抛异常。
     */
    @Test
    void invalidOrderIdIsRejectedWithoutThrowing() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = matcher(h);

        m.onCommand(Command.newOrder(1, 0, 11, SYMBOL, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100, 5));

        assertEquals(0, m.liveOrderCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 0 && o.status().equals("REJECTED")
                && o.rejectReason().equals("INVALID_ORDER")));
    }

    /**
     * 验证对象池耗尽时会明确拒绝，而不是在热路径隐式分配新订单对象。
     */
    @Test
    void capacityExhaustionRejectsWithoutRuntimeOrderAllocation() {
        RecordingHandler h = new RecordingHandler();
        UltraLowLatencyMatcher m = new UltraLowLatencyMatcher(
                new MatcherConfig(SYMBOL, 8, 16, 1, 100_000_000L, true), h);

        m.onCommand(limit(1, 101, 11, Side.SELL, 100, 5));
        m.onCommand(Command.newOrder(2, 201, 21, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 100, 1));

        assertEquals(0, h.trades.size());
        assertEquals(1, m.liveOrderCount());
        assertEquals(1, m.stats().capacityRejectedCommandCount());
        assertEquals(1, m.stats().orderPoolExhaustedCount());
        assertTrue(h.orders.stream().anyMatch(o -> o.orderId() == 201 && o.status().equals("REJECTED")
                && o.rejectReason().equals("CAPACITY_EXCEEDED")));
    }

}
