package io.github.ike.ullmatcher.core;

import io.github.ike.ullmatcher.api.*;
import io.github.ike.ullmatcher.book.FastOrderBook;
import io.github.ike.ullmatcher.metrics.MatcherStats;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 单交易对、单线程撮合引擎。
 * <p>
 * 高频交易规则：
 * <ul>
 *     <li>必须由一个固定平台线程调用，不要放进虚拟线程。</li>
 *     <li>热路径不访问数据库、Redis、HTTP、Kafka 等任何阻塞输入输出。</li>
 *     <li>撮合线程只产生成交事件和订单状态事件，不直接扣资产。</li>
 *     <li>价格、数量、金额全部使用 {@code long} 定点数。</li>
 * </ul>
 */
public final class UltraLowLatencyMatcher {
    /** 不可变撮合器配置。 */
    private final MatcherConfig cfg;

    /** 单线程订单簿。 */
    private final FastOrderBook book;

    /** 可复用订单节点池。 */
    private final OrderPool pool;

    /** 撮合线程内调用的事件回调。 */
    private final MatchEventHandler handler;

    /** 复用成交事件对象，避免热路径分配。 */
    private final TradeEvent trade = new TradeEvent();

    /** 复用订单事件对象，避免热路径分配。 */
    private final OrderEvent orderEvent = new OrderEvent();

    /** 最后已应用的命令序列号。 */
    private long lastSequence;

    /** 撮合器本地成交编号序列。 */
    private long tradeSequence;

    /** 已接收命令数量。 */
    private long commandCount;

    /** 已发出订单事件数量。 */
    private long orderEventCount;

    /** 已拒绝命令数量。 */
    private long rejectedCommandCount;

    /** 容量不足拒绝数量。 */
    private long capacityRejectedCommandCount;

    /**
     * 创建撮合器。
     *
     * @param cfg 撮合器配置
     * @param handler 在撮合线程内同步调用的事件处理器
     */
    public UltraLowLatencyMatcher(MatcherConfig cfg, MatchEventHandler handler) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.book = new FastOrderBook(cfg.expectedPriceLevels(), cfg.expectedLiveOrders());
        this.pool = new OrderPool(cfg.orderPoolSize());
        this.handler = Objects.requireNonNull(handler, "handler");
    }

    /**
     * 向撮合器应用一条命令。
     *
     * @param c 带严格递增序列号的命令
     */
    public void onCommand(Command c) {
        Objects.requireNonNull(c, "command");
        commandCount++;
        if (c.sequence <= lastSequence) {
            reject(c, RejectReason.DUPLICATE_SEQUENCE);
            return;
        }
        lastSequence = c.sequence;
        switch (c.type) {
            case NEW_ORDER -> onNewOrder(c);
            case CANCEL_ORDER -> onCancel(c);
            case SNAPSHOT_MARKER -> emitOrder(c.sequence, c.symbolId, 0, OrderStatus.NEW, RejectReason.NONE, 0);
            case SHUTDOWN -> { /* 外层循环处理 */ }
        }
    }

    /**
     * 处理新订单命令。
     *
     * @param c 新订单命令
     */
    private void onNewOrder(Command c) {
        if (c.orderId <= 0 || c.userId <= 0 || c.quantity <= 0 || c.price < 0 ||
                quoteAmountWouldOverflow(c.price, c.quantity) || c.symbolId != cfg.symbolId() ||
                (c.side != Side.BUY.code && c.side != Side.SELL.code)) {
            reject(c, RejectReason.INVALID_ORDER);
            return;
        }
        if (book.exists(c.orderId)) {
            reject(c, RejectReason.DUPLICATE_ORDER_ID);
            return;
        }

        if (willRestIfUnfilled(c.timeInForce) && book.lacksRestingCapacity(c.side, c.price, c.orderId)) {
            rejectCapacity(c);
            return;
        }

        Order taker = pool.borrow();
        if (taker == null) {
            rejectCapacity(c);
            return;
        }
        taker.orderId = c.orderId;
        taker.userId = c.userId;
        taker.symbolId = c.symbolId;
        taker.side = c.side;
        taker.timeInForce = c.timeInForce;
        taker.price = c.price;
        taker.quantity = c.quantity;
        taker.remaining = c.quantity;
        taker.sequence = c.sequence;
        taker.expireAtEpochMillis = c.expireAtEpochMillis;

        Order opposite = bestOpposite(taker);
        boolean crossesBest = opposite != null && crosses(taker, opposite);
        if (cfg.preventSelfTrade() && crossesBest &&
                book.hasSelfTradeInFillPath(taker.side, taker.price, taker.userId, taker.quantity)) {
            emitOrder(c, OrderStatus.REJECTED, RejectReason.SELF_TRADE_PREVENTED, c.quantity);
            pool.release(taker);
            return;
        }
        if (c.timeInForce == TimeInForce.POST_ONLY.code && crossesBest) {
            emitOrder(c, OrderStatus.REJECTED, RejectReason.POST_ONLY_WOULD_TAKE, c.quantity);
            pool.release(taker);
            return;
        }
        if (c.timeInForce == TimeInForce.FOK.code &&
                !book.hasFillableQuantity(taker.side, taker.price, taker.quantity, taker.userId, cfg.preventSelfTrade())) {
            emitOrder(c, OrderStatus.CANCELLED, RejectReason.FOK_NOT_FILLABLE, c.quantity);
            pool.release(taker);
            return;
        }

        match(taker);

        if (taker.remaining > 0 && taker.timeInForce != TimeInForce.IOC.code && taker.timeInForce != TimeInForce.FOK.code) {
            if (book.add(taker)) {
                emitOrder(taker,
                        taker.remaining == taker.quantity ? OrderStatus.NEW : OrderStatus.PARTIALLY_FILLED,
                        RejectReason.NONE, taker.remaining);
            } else {
                emitOrder(taker, OrderStatus.CANCELLED, RejectReason.CAPACITY_EXCEEDED, taker.remaining);
                capacityRejectedCommandCount++;
                pool.release(taker);
            }
        } else {
            if (taker.remaining == 0) {
                emitOrder(taker, OrderStatus.FILLED, RejectReason.NONE, 0);
            } else {
                emitOrder(taker,
                        taker.remaining == taker.quantity ? OrderStatus.CANCELLED : OrderStatus.PARTIALLY_FILLED,
                        RejectReason.NONE, taker.remaining);
            }
            pool.release(taker);
        }
    }

    /**
     * 当吃单方价格穿透订单簿时，与对手方挂单流动性撮合。
     *
     * @param taker 尚未进入订单簿的来单
     */
    private void match(Order taker) {
        while (taker.remaining > 0) {
            Order maker = bestOpposite(taker);
            if (maker == null || !crosses(taker, maker)) return;
            if (cfg.preventSelfTrade() && taker.userId == maker.userId) return;
            if (isBuy(taker)) {
                trade(taker, maker, maker.price, taker);
            } else {
                trade(maker, taker, maker.price, taker);
            }
        }
    }

    /**
     * 执行一笔成交并发出挂单方状态事件。
     *
     * @param buy 买单
     * @param sell 卖单
     * @param price 成交价格
     * @param taker 本次撮合中的来单
     */
    private void trade(Order buy, Order sell, long price, Order taker) {
        long qty = Math.min(buy.remaining, sell.remaining);
        Order maker = buy == taker ? sell : buy;
        book.decreaseQuantity(maker, qty);
        buy.remaining -= qty;
        sell.remaining -= qty;

        trade.sequence = lastSequence;
        trade.tradeId = ++tradeSequence;
        trade.symbolId = cfg.symbolId();
        trade.buyOrderId = buy.orderId;
        trade.sellOrderId = sell.orderId;
        trade.buyerUserId = buy.userId;
        trade.sellerUserId = sell.userId;
        trade.price = price;
        trade.quantity = qty;
        trade.quoteAmount = quoteAmount(price, qty);
        handler.onTrade(trade);

        if (buy != taker) {
            if (buy.remaining == 0) finishOrRemove(buy);
            else emitOrder(buy, OrderStatus.PARTIALLY_FILLED, RejectReason.NONE, buy.remaining);
        }
        if (sell != taker) {
            if (sell.remaining == 0) finishOrRemove(sell);
            else emitOrder(sell, OrderStatus.PARTIALLY_FILLED, RejectReason.NONE, sell.remaining);
        }
    }

    /**
     * 为挂单发出完全成交事件，并在需要时从订单簿移除。
     *
     * @param o 剩余数量归零的订单
     */
    private void finishOrRemove(Order o) {
        boolean resting = book.get(o.orderId) != null;
        if (resting) book.remove(o);
        emitOrder(o, OrderStatus.FILLED, RejectReason.NONE, 0);
        if (resting) pool.release(o);
    }

    /**
     * 处理撤单命令。
     *
     * @param c 撤单命令
     */
    private void onCancel(Command c) {
        if (c.orderId <= 0 || c.symbolId != cfg.symbolId()) {
            reject(c, RejectReason.INVALID_ORDER);
            return;
        }
        Order o = book.get(c.orderId);
        if (o == null) {
            reject(c, RejectReason.UNKNOWN_ORDER);
            return;
        }
        long remaining = o.remaining;
        book.remove(o);
        pool.release(o);
        emitOrder(c.sequence, c.symbolId, c.orderId, OrderStatus.CANCELLED, RejectReason.NONE, remaining);
    }

    /**
     * 为命令发出拒绝事件。
     *
     * @param c 被拒绝的命令
     * @param reason 拒绝原因
     */
    private void reject(Command c, RejectReason reason) {
        rejectedCommandCount++;
        emitOrder(c, OrderStatus.REJECTED, reason, c.quantity);
    }

    /**
     * 发出容量不足拒绝事件。
     *
     * @param c 被拒绝命令
     */
    private void rejectCapacity(Command c) {
        capacityRejectedCommandCount++;
        reject(c, RejectReason.CAPACITY_EXCEEDED);
    }

    /**
     * 填充并发出可复用订单事件。
     *
     * @param seq 命令序列号
     * @param symbolId 交易对或分片编号
     * @param orderId 受影响订单编号
     * @param status 处理后的订单状态
     * @param reason 拒绝原因，非拒绝事件为 {@link RejectReason#NONE}
     * @param remaining 剩余未成交数量
     */
    private void emitOrder(long seq, int symbolId, long orderId, OrderStatus status, RejectReason reason, long remaining) {
        emitOrder(seq, symbolId, orderId, status, reason, remaining, 0L);
    }

    private void emitOrder(Command c, OrderStatus status, RejectReason reason, long remaining) {
        emitOrder(c.sequence, c.symbolId, c.orderId, status, reason, remaining, c.expireAtEpochMillis,
                c.side, OrderType.LIMIT.code, c.timeInForce, c.price, c.quantity);
    }

    private void emitOrder(Order order, OrderStatus status, RejectReason reason, long remaining) {
        emitOrder(lastSequence, order.symbolId, order.orderId, status, reason, remaining, order.expireAtEpochMillis,
                order.side, OrderType.LIMIT.code, order.timeInForce, order.price, order.quantity);
    }

    private void emitOrder(long seq, int symbolId, long orderId, OrderStatus status, RejectReason reason, long remaining,
                           long expireAtEpochMillis) {
        emitOrder(seq, symbolId, orderId, status, reason, remaining, expireAtEpochMillis,
                (byte) 0, (byte) 0, (byte) 0, 0L, 0L);
    }

    private void emitOrder(long seq, int symbolId, long orderId, OrderStatus status, RejectReason reason, long remaining,
                           long expireAtEpochMillis, byte side, byte orderType, byte timeInForce, long price, long quantity) {
        orderEvent.sequence = seq;
        orderEvent.symbolId = symbolId;
        orderEvent.orderId = orderId;
        orderEvent.status = status;
        orderEvent.rejectReason = reason;
        orderEvent.remaining = remaining;
        orderEvent.side = side;
        orderEvent.orderType = orderType;
        orderEvent.timeInForce = timeInForce;
        orderEvent.price = price;
        orderEvent.quantity = quantity;
        orderEvent.expireAtEpochMillis = expireAtEpochMillis;
        orderEventCount++;
        handler.onOrder(orderEvent);
    }

    /**
     * 返回吃单方对应的最优对手方订单。
     *
     * @param taker 来单
     * @return 最优对手方挂单；不存在时返回 {@code null}
     */
    private Order bestOpposite(Order taker) {
        return isBuy(taker) ? book.bestAsk() : book.bestBid();
    }

    /**
     * 检查吃单方是否穿透对手方挂单。
     *
     * @param taker 来单
     * @param opposite 对手方挂单
     * @return 价格穿透时返回 {@code true}
     */
    private boolean crosses(Order taker, Order opposite) {
        return isBuy(taker) ? taker.price >= opposite.price : taker.price <= opposite.price;
    }

    /**
     * 检查订单是否为买方。
     *
     * @param order 待检查订单
     * @return 订单方向为买方时返回 {@code true}
     */
    private boolean isBuy(Order order) {
        return order.side == Side.BUY.code;
    }

    /**
     * 判断订单剩余数量是否可能进入订单簿。
     *
     * @param timeInForce 有效期策略编码
     * @return 可能挂单时返回 {@code true}
     */
    private boolean willRestIfUnfilled(byte timeInForce) {
        return timeInForce != TimeInForce.IOC.code && timeInForce != TimeInForce.FOK.code;
    }

    /**
     * 快照读取入口：只允许在撮合线程暂停或通过控制命令同步后调用。
     *
     * @param consumer 活跃订单消费者
     */
    public void forEachLiveOrder(Consumer<Order> consumer) {
        book.forEachOrder(consumer);
    }

    /**
     * 快照恢复入口：直接恢复一个活跃挂单，不触发撮合和事件回调。
     * <p>
     * 只能在新建撮合器且尚未处理实时命令时调用。
     *
     * @param orderId 订单编号
     * @param userId 用户编号
     * @param symbolId 交易对或分片编号
     * @param side 方向编码
     * @param timeInForce 有效期策略编码
     * @param price 定点数价格
     * @param quantity 原始定点数数量
     * @param remaining 剩余定点数数量
     * @param sequence 创建该订单的命令序列号
     */
    public void restoreLiveOrder(long orderId, long userId, int symbolId, byte side, byte timeInForce,
                                 long price, long quantity, long remaining, long sequence, long expireAtEpochMillis) {
        if (symbolId != cfg.symbolId() || price < 0 || quantity <= 0 || remaining <= 0 || remaining > quantity ||
                quoteAmountWouldOverflow(price, quantity) ||
                (side != Side.BUY.code && side != Side.SELL.code) || book.exists(orderId)) {
            throw new IllegalArgumentException("invalid snapshot order " + orderId);
        }
        Order order = pool.borrow();
        if (order == null) {
            throw new IllegalStateException("order pool exhausted while restoring snapshot");
        }
        order.orderId = orderId;
        order.userId = userId;
        order.symbolId = symbolId;
        order.side = side;
        order.timeInForce = timeInForce;
        order.price = price;
        order.quantity = quantity;
        order.remaining = remaining;
        order.sequence = sequence;
        order.expireAtEpochMillis = expireAtEpochMillis;
        if (!book.add(order)) {
            pool.release(order);
            throw new IllegalStateException("order book capacity exhausted while restoring snapshot");
        }
    }

    /**
     * 快照恢复入口：恢复撮合器序列状态，不触发事件回调。
     *
     * @param lastSequence 快照包含的最后命令序列号
     * @param lastTradeId 快照包含的最后成交编号
     */
    public void restoreSequenceState(long lastSequence, long lastTradeId) {
        if (lastSequence < 0 || lastTradeId < 0) {
            throw new IllegalArgumentException("snapshot sequence state must be non-negative");
        }
        this.lastSequence = lastSequence;
        this.tradeSequence = lastTradeId;
    }

    /**
     * 返回最后已应用的命令序列号。
     *
     * @return 最后已应用序列号
     */
    public long lastSequence() {
        return lastSequence;
    }

    /**
     * 返回最后已分配的撮合器本地成交编号。
     *
     * @return 最后成交编号
     */
    public long lastTradeId() {
        return tradeSequence;
    }

    /**
     * 返回活跃订单数量。
     *
     * @return 活跃订单数量
     */
    public long liveOrderCount() {
        return book.orderCount();
    }

    /**
     * 生成当前撮合器运行指标快照。
     *
     * @return 指标快照
     */
    public MatcherStats stats() {
        return new MatcherStats(lastSequence, tradeSequence, book.orderCount(), commandCount, tradeSequence,
                orderEventCount, rejectedCommandCount, capacityRejectedCommandCount, pool.available(),
                pool.exhaustedBorrows(), book.availablePriceLevels());
    }

    /**
     * 检查最大计价金额计算是否会溢出。
     *
     * @param price 定点数价格
     * @param quantity 定点数数量
     * @return 会溢出时返回 {@code true}
     */
    private boolean quoteAmountWouldOverflow(long price, long quantity) {
        return price != 0 && quantity > Long.MAX_VALUE / price;
    }

    private long quoteAmount(long price, long quantity) {
        if (quoteAmountWouldOverflow(price, quantity)) {
            throw new IllegalStateException("quote amount overflow after order validation");
        }
        return price * quantity / cfg.quoteScale();
    }
}
