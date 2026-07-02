package io.github.ike.ullmatcher.core;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.TtlEvent;
import io.github.ike.ullmatcher.api.TradeEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * 将可复用撮合事件复制成快照的测试事件处理器。
 */
final class RecordingHandler implements MatchEventHandler {
    /** 已捕获的成交快照。 */
    final List<TradeSnapshot> trades = new ArrayList<>();

    /** 已捕获的订单快照。 */
    final List<OrderSnapshot> orders = new ArrayList<>();

    /** 已捕获的 TTL 事件快照。 */
    final List<TtlSnapshot> ttlEvents = new ArrayList<>();

    /**
     * 捕获成交事件。
     *
     * @param e 撮合器发出的可复用成交事件
     */
    @Override
    public void onTrade(TradeEvent e) {
        trades.add(new TradeSnapshot(e.sequence, e.tradeId, e.symbolId, e.buyOrderId, e.sellOrderId,
                e.buyerUserId, e.sellerUserId, e.price, e.quantity, e.quoteAmount));
    }

    /**
     * 捕获订单事件。
     *
     * @param e 撮合器发出的可复用订单事件
     */
    @Override
    public void onOrder(OrderEvent e) {
        orders.add(new OrderSnapshot(e.sequence, e.symbolId, e.orderId, e.status.name(),
                e.rejectReason.name(), e.remaining));
    }

    @Override
    public void onTtl(TtlEvent e) {
        ttlEvents.add(new TtlSnapshot(
                e.eventTimeEpochMillis,
                e.symbolId,
                e.orderId,
                e.action.name(),
                e.source,
                e.detail,
                e.expireAtEpochMillis
        ));
    }

    /**
     * 用于断言的不可变成交事件快照。
     *
     * @param sequence 命令序列号
     * @param tradeId 成交编号
     * @param symbolId 交易对编号
     * @param buyOrderId 买单编号
     * @param sellOrderId 卖单编号
     * @param buyerUserId 买方用户编号
     * @param sellerUserId 卖方用户编号
     * @param price 成交价格
     * @param quantity 成交数量
     * @param quoteAmount 计价金额
     */
    record TradeSnapshot(long sequence, long tradeId, int symbolId, long buyOrderId, long sellOrderId,
                         long buyerUserId, long sellerUserId, long price, long quantity, long quoteAmount) {}

    /**
     * 用于断言的不可变订单事件快照。
     *
     * @param sequence 命令序列号
     * @param symbolId 交易对编号
     * @param orderId 订单编号
     * @param status 订单状态名称
     * @param rejectReason 拒绝原因名称
     * @param remaining 剩余数量
     */
    record OrderSnapshot(long sequence, int symbolId, long orderId, String status, String rejectReason, long remaining) {}

    record TtlSnapshot(long eventTimeEpochMillis, int symbolId, long orderId, String action,
                       String source, String detail, long expireAtEpochMillis) {}
}
