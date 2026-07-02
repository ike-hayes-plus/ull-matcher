package io.github.ike.ullmatcher.hft;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderStatus;
import io.github.ike.ullmatcher.api.RejectReason;
import io.github.ike.ullmatcher.api.TtlEvent;
import io.github.ike.ullmatcher.api.TtlEventAction;
import io.github.ike.ullmatcher.api.TradeEvent;

/**
 * 固定容量的异步事件转发器。
 * <p>
 * 撮合线程调用 {@link #onTrade(TradeEvent)} 和 {@link #onOrder(OrderEvent)} 时只复制字段到预分配槽位；
 * 下游线程通过 {@link #drainTo(MatchEventHandler, int)} 批量转发。事件队列满时会抛出异常，
 * 由 {@code MatchLoop} 捕获并停止撮合循环，避免事件丢失后继续撮合。
 */
public final class AsyncEventDispatcher implements MatchEventHandler {
    /** 成交事件类型。 */
    private static final byte TRADE = 1;

    /** 订单事件类型。 */
    private static final byte ORDER = 2;

    /** TTL 事件类型。 */
    private static final byte TTL = 3;

    /** 事件槽位。 */
    private final EventSlot[] slots;

    /** 环形下标掩码。 */
    private final int mask;

    /** 下一个生产者序列号。 */
    private volatile long producerSeq;

    /** 下一个消费者序列号。 */
    private volatile long consumerSeq;

    /** 消费线程复用成交事件对象。 */
    private final TradeEvent trade = new TradeEvent();

    /** 消费线程复用订单事件对象。 */
    private final OrderEvent order = new OrderEvent();

    /** 消费线程复用 TTL 事件对象。 */
    private final TtlEvent ttl = new TtlEvent();

    /** 队列满失败次数。 */
    private long fullFailureCount;

    /**
     * 创建异步事件转发器。
     *
     * @param capacityPowerOfTwo 事件槽位容量，必须是 2 的幂
     */
    public AsyncEventDispatcher(int capacityPowerOfTwo) {
        if (Integer.bitCount(capacityPowerOfTwo) != 1) {
            throw new IllegalArgumentException("capacity must be power of two");
        }
        slots = new EventSlot[capacityPowerOfTwo];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new EventSlot();
        }
        mask = capacityPowerOfTwo - 1;
    }

    /**
     * 复制成交事件到异步队列。
     *
     * @param event 撮合线程内复用成交事件
     */
    @Override
    public void onTrade(TradeEvent event) {
        EventSlot slot = claim();
        slot.type = TRADE;
        slot.sequence = event.sequence;
        slot.tradeId = event.tradeId;
        slot.symbolId = event.symbolId;
        slot.buyOrderId = event.buyOrderId;
        slot.sellOrderId = event.sellOrderId;
        slot.buyerUserId = event.buyerUserId;
        slot.sellerUserId = event.sellerUserId;
        slot.price = event.price;
        slot.quantity = event.quantity;
        slot.quoteAmount = event.quoteAmount;
        publish();
    }

    /**
     * 复制订单事件到异步队列。
     *
     * @param event 撮合线程内复用订单事件
     */
    @Override
    public void onOrder(OrderEvent event) {
        EventSlot slot = claim();
        slot.type = ORDER;
        slot.sequence = event.sequence;
        slot.symbolId = event.symbolId;
        slot.orderId = event.orderId;
        slot.orderStatus = event.status;
        slot.rejectReason = event.rejectReason;
        slot.remaining = event.remaining;
        slot.expireAtEpochMillis = event.expireAtEpochMillis;
        publish();
    }

    @Override
    public void onTtl(TtlEvent event) {
        EventSlot slot = claim();
        slot.type = TTL;
        slot.eventTimeEpochMillis = event.eventTimeEpochMillis;
        slot.symbolId = event.symbolId;
        slot.orderId = event.orderId;
        slot.ttlAction = event.action;
        slot.source = event.source;
        slot.detail = event.detail;
        slot.expireAtEpochMillis = event.expireAtEpochMillis;
        publish();
    }

    /**
     * 从异步队列转发最多指定数量的事件。
     *
     * @param downstream 下游事件处理器
     * @param limit 最多转发数量
     * @return 实际转发数量
     */
    public int drainTo(MatchEventHandler downstream, int limit) {
        int drained = 0;
        while (drained < limit) {
            long c = consumerSeq;
            if (c >= producerSeq) break;
            EventSlot slot = slots[(int) (c & mask)];
            if (slot.type == TRADE) {
                fillTrade(slot);
                downstream.onTrade(trade);
            } else if (slot.type == ORDER) {
                fillOrder(slot);
                downstream.onOrder(order);
            } else if (slot.type == TTL) {
                fillTtl(slot);
                downstream.onTtl(ttl);
            } else {
                throw new IllegalStateException("unknown event slot type " + slot.type);
            }
            slot.type = 0;
            consumerSeq = c + 1;
            drained++;
        }
        return drained;
    }

    /**
     * 返回当前排队事件数量。
     *
     * @return 事件数量
     */
    public int size() {
        return (int) (producerSeq - consumerSeq);
    }

    /**
     * 返回队列满失败次数。
     *
     * @return 队列满失败次数
     */
    public long fullFailureCount() {
        return fullFailureCount;
    }

    /**
     * 申请一个生产槽位。
     *
     * @return 可写槽位
     */
    private EventSlot claim() {
        long p = producerSeq;
        if (p - consumerSeq >= slots.length) {
            fullFailureCount++;
            throw new IllegalStateException("match event outbox is full");
        }
        return slots[(int) (p & mask)];
    }

    /**
     * 发布刚写好的槽位。
     */
    private void publish() {
        producerSeq++;
    }

    /**
     * 填充复用成交事件。
     *
     * @param slot 事件槽位
     */
    private void fillTrade(EventSlot slot) {
        trade.sequence = slot.sequence;
        trade.tradeId = slot.tradeId;
        trade.symbolId = slot.symbolId;
        trade.buyOrderId = slot.buyOrderId;
        trade.sellOrderId = slot.sellOrderId;
        trade.buyerUserId = slot.buyerUserId;
        trade.sellerUserId = slot.sellerUserId;
        trade.price = slot.price;
        trade.quantity = slot.quantity;
        trade.quoteAmount = slot.quoteAmount;
    }

    /**
     * 填充复用订单事件。
     *
     * @param slot 事件槽位
     */
    private void fillOrder(EventSlot slot) {
        order.sequence = slot.sequence;
        order.symbolId = slot.symbolId;
        order.orderId = slot.orderId;
        order.status = slot.orderStatus;
        order.rejectReason = slot.rejectReason;
        order.remaining = slot.remaining;
        order.expireAtEpochMillis = slot.expireAtEpochMillis;
    }

    private void fillTtl(EventSlot slot) {
        ttl.eventTimeEpochMillis = slot.eventTimeEpochMillis;
        ttl.symbolId = slot.symbolId;
        ttl.orderId = slot.orderId;
        ttl.action = slot.ttlAction;
        ttl.source = slot.source;
        ttl.detail = slot.detail;
        ttl.expireAtEpochMillis = slot.expireAtEpochMillis;
    }

    /**
     * 预分配事件槽位。
     */
    private static final class EventSlot {
        /** 事件类型。 */
        byte type;

        /** 命令序列号。 */
        long sequence;

        /** TTL 事件时间。 */
        long eventTimeEpochMillis;

        /** 成交编号。 */
        long tradeId;

        /** 交易对或分片编号。 */
        int symbolId;

        /** 买单编号。 */
        long buyOrderId;

        /** 卖单编号。 */
        long sellOrderId;

        /** 买方用户编号。 */
        long buyerUserId;

        /** 卖方用户编号。 */
        long sellerUserId;

        /** 订单编号。 */
        long orderId;

        /** 成交价格。 */
        long price;

        /** 成交数量。 */
        long quantity;

        /** 计价金额。 */
        long quoteAmount;

        /** 订单状态。 */
        OrderStatus orderStatus;

        /** 拒绝原因。 */
        RejectReason rejectReason;

        /** 剩余数量。 */
        long remaining;

        /** TTL 动作。 */
        TtlEventAction ttlAction;

        /** TTL 来源。 */
        String source;

        /** TTL 详情。 */
        String detail;

        /** TTL 过期时间。 */
        long expireAtEpochMillis;
    }
}
