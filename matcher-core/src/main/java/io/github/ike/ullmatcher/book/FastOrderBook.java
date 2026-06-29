package io.github.ike.ullmatcher.book;

import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.core.Order;
import io.github.ike.ullmatcher.core.PriceLevel;
import io.github.ike.ullmatcher.util.LongHeap;
import io.github.ike.ullmatcher.util.LongObjectHashMap;

import java.util.function.Consumer;

/**
 * 单线程订单簿，面向极低延迟撮合场景。
 * <p>
 * 核心结构：
 * <ul>
 *     <li>买盘和卖盘：{@code long price -> PriceLevel}，避免 {@code BigDecimal} 和装箱。</li>
 *     <li>订单索引：{@code long orderId -> Order}，用于 {@code O(1)} 撤单。</li>
 *     <li>买盘价格堆和卖盘价格堆：原始 {@code long} 堆，使用懒删除查找最优价。</li>
 * </ul>
 * <p>
 * 注意：
 * 本类不是线程安全的，只能在一个撮合线程内使用。
 */
public final class FastOrderBook {
    /** 按定点数价格索引的买盘价格档。 */
    private final LongObjectHashMap<PriceLevel> bids;

    /** 按定点数价格索引的卖盘价格档。 */
    private final LongObjectHashMap<PriceLevel> asks;

    /** 按订单编号索引的活跃订单。 */
    private final LongObjectHashMap<Order> orders;

    /** 买盘价格最大堆；过期价格采用懒删除。 */
    private final LongHeap bidPrices;

    /** 卖盘价格最小堆；过期价格采用懒删除。 */
    private final LongHeap askPrices;

    /** 买盘扫描堆复用缓冲，避免 FOK/STP 预检查分配对象。 */
    private final LongHeap bidScanPrices;

    /** 卖盘扫描堆复用缓冲，避免 FOK/STP 预检查分配对象。 */
    private final LongHeap askScanPrices;

    /** 价格档对象池。 */
    private final PriceLevel[] priceLevelPool;

    /** 价格档对象池栈顶后一位索引。 */
    private int priceLevelTop;

    /**
     * 创建带预分配索引的订单簿。
     *
     * @param expectedPriceLevels 预期活跃价格档数量
     * @param expectedOrders 预期活跃订单数量
     */
    public FastOrderBook(int expectedPriceLevels, int expectedOrders) {
        bids = new LongObjectHashMap<>(expectedPriceLevels);
        asks = new LongObjectHashMap<>(expectedPriceLevels);
        orders = new LongObjectHashMap<>(expectedOrders);
        bidPrices = new LongHeap(expectedPriceLevels, true);
        askPrices = new LongHeap(expectedPriceLevels, false);
        bidScanPrices = new LongHeap(expectedPriceLevels, true);
        askScanPrices = new LongHeap(expectedPriceLevels, false);
        int priceLevelCapacity = Math.max(2, expectedPriceLevels << 1);
        priceLevelPool = new PriceLevel[priceLevelCapacity];
        for (int i = 0; i < priceLevelPool.length; i++) {
            priceLevelPool[i] = new PriceLevel();
        }
        priceLevelTop = priceLevelPool.length;
    }

    /**
     * 将挂单加入对应方向和价格档。
     *
     * @param o 需要索引并挂接的订单
     * @return 成功加入时返回 {@code true}；容量不足时返回 {@code false}
     */
    public boolean add(Order o) {
        if (lacksRestingCapacity(o.side, o.price, o.orderId)) return false;
        LongObjectHashMap<PriceLevel> side = levelsFor(o.side);
        PriceLevel level = side.get(o.price);
        if (level == null) {
            level = borrowPriceLevel(o.price);
            side.put(o.price, level);
            LongHeap prices = pricesFor(o.side);
            if (!prices.hasCapacity()) {
                side.remove(o.price);
                releasePriceLevel(level);
                return false;
            }
            prices.add(o.price);
        }
        level.addLast(o);
        orders.put(o.orderId, o);
        return true;
    }

    /**
     * 按价格时间优先返回最优买单。
     *
     * @return 最优买单；没有买盘时返回 {@code null}
     */
    public Order bestBid() {
        PriceLevel l = bestBidLevel();
        return l == null ? null : l.head;
    }

    /**
     * 按价格时间优先返回最优卖单。
     *
     * @return 最优卖单；没有卖盘时返回 {@code null}
     */
    public Order bestAsk() {
        PriceLevel l = bestAskLevel();
        return l == null ? null : l.head;
    }

    /**
     * 返回最优非空买盘价格档。
     *
     * @return 最优买盘价格档；没有买盘时返回 {@code null}
     */
    public PriceLevel bestBidLevel() {
        return bestLevel(bids, bidPrices);
    }

    /**
     * 返回最优非空卖盘价格档。
     *
     * @return 最优卖盘价格档；没有卖盘时返回 {@code null}
     */
    public PriceLevel bestAskLevel() {
        return bestLevel(asks, askPrices);
    }

    /**
     * 按订单编号查找活跃订单。
     *
     * @param orderId 订单编号
     * @return 活跃订单；不存在时返回 {@code null}
     */
    public Order get(long orderId) {
        return orders.get(orderId);
    }

    /**
     * 从订单簿和订单索引中移除挂单。
     *
     * @param o 需要移除的活跃挂单
     */
    public void remove(Order o) {
        LongObjectHashMap<PriceLevel> side = levelsFor(o.side);
        PriceLevel level = side.get(o.price);
        if (level == null) return;
        level.remove(o);
        orders.remove(o.orderId);
        if (level.isEmpty()) {
            side.remove(o.price); // 堆里价格档采用懒删除，避免 O(logN) 删除成本
            releasePriceLevel(level);
        }
    }

    /**
     * 判断订单编号当前是否活跃。
     *
     * @param orderId 订单编号
     * @return 订单存在时返回 {@code true}
     */
    public boolean exists(long orderId) {
        return orders.containsKey(orderId);
    }

    /**
     * 从订单所在价格档扣减已成交数量。
     *
     * @param order 已挂在订单簿中的挂单
     * @param quantity 已成交数量
     */
    public void decreaseQuantity(Order order, long quantity) {
        LongObjectHashMap<PriceLevel> side = levelsFor(order.side);
        PriceLevel level = side.get(order.price);
        if (level == null) {
            throw new IllegalStateException("missing price level for order " + order.orderId);
        }
        level.decreaseQuantity(quantity);
    }

    /**
     * 检查对手方流动性是否足够完全成交 FOK 订单。
     * <p>
     * 扫描按价格时间优先执行；启用防自成交时，如果填满路径中会遇到同用户挂单，则返回 {@code false}。
     *
     * @param takerSide 吃单方方向编码
     * @param limitPrice 吃单方限价
     * @param requiredQty 所需成交数量
     * @param takerUserId 吃单方用户编号
     * @param preventSelfTrade 是否启用防自成交
     * @return 订单簿可完全满足所需数量时返回 {@code true}
     */
    public boolean hasFillableQuantity(byte takerSide, long limitPrice, long requiredQty,
                                       long takerUserId, boolean preventSelfTrade) {
        PriceLevel bestLevel = bestOppositeLevelFor(takerSide);
        if (bestLevel == null || !crosses(takerSide, limitPrice, bestLevel.price)) {
            return false;
        }
        if (requiredQty <= bestLevel.totalQuantity) {
            return levelHasFillableQuantity(bestLevel, requiredQty, takerUserId, preventSelfTrade);
        }

        long remaining = requiredQty;
        LongObjectHashMap<PriceLevel> side = oppositeLevelsFor(takerSide);
        LongHeap prices = oppositeScanPricesFor(takerSide);
        oppositePricesFor(takerSide).copyInto(prices);
        boolean hasLastPrice = false;
        long lastPrice = 0;
        while (remaining > 0 && !prices.isEmpty()) {
            long price = prices.poll();
            if (hasLastPrice && price == lastPrice) continue;
            hasLastPrice = true;
            lastPrice = price;

            PriceLevel level = side.get(price);
            if (level == null || level.isEmpty()) continue;
            if (!crosses(takerSide, limitPrice, price)) break;

            for (Order order = level.head; order != null; order = order.next) {
                if (preventSelfTrade && order.userId == takerUserId) return false;
                remaining -= order.remaining;
                if (remaining <= 0) return true;
            }
        }
        return false;
    }

    /**
     * 检查一笔订单按价格时间优先撮合时是否会遇到同用户挂单。
     *
     * @param takerSide 吃单方方向编码
     * @param limitPrice 吃单方限价
     * @param takerUserId 吃单方用户编号
     * @param quantity 吃单方数量
     * @return 会与同用户挂单相遇时返回 {@code true}
     */
    public boolean hasSelfTradeInFillPath(byte takerSide, long limitPrice, long takerUserId, long quantity) {
        PriceLevel bestLevel = bestOppositeLevelFor(takerSide);
        if (bestLevel == null || !crosses(takerSide, limitPrice, bestLevel.price)) {
            return false;
        }
        if (quantity <= bestLevel.totalQuantity) {
            return levelHasSelfTradeBeforeFilled(bestLevel, takerUserId, quantity);
        }

        long remaining = quantity;
        LongObjectHashMap<PriceLevel> side = oppositeLevelsFor(takerSide);
        LongHeap prices = oppositeScanPricesFor(takerSide);
        oppositePricesFor(takerSide).copyInto(prices);
        boolean hasLastPrice = false;
        long lastPrice = 0;
        while (remaining > 0 && !prices.isEmpty()) {
            long price = prices.poll();
            if (hasLastPrice && price == lastPrice) continue;
            hasLastPrice = true;
            lastPrice = price;

            PriceLevel level = side.get(price);
            if (level == null || level.isEmpty()) continue;
            if (!crosses(takerSide, limitPrice, price)) break;

            for (Order order = level.head; order != null; order = order.next) {
                if (order.userId == takerUserId) return true;
                remaining -= order.remaining;
                if (remaining <= 0) return false;
            }
        }
        return false;
    }

    /**
     * 遍历所有活跃订单用于生成快照。
     * <p>
     * 只能在撮合线程内或撮合器停止后调用。
     *
     * @param consumer 订单消费者
     */
    public void forEachOrder(Consumer<Order> consumer) {
        orders.forEachValue(consumer);
    }

    /**
     * 返回活跃订单数量。
     *
     * @return 活跃订单数量
     */
    public long orderCount() {
        return orders.size();
    }

    /**
     * 判断订单能否在不触发运行时扩容的前提下进入订单簿。
     *
     * @param side 订单方向编码
     * @param price 订单价格
     * @param orderId 订单编号
     * @return 容量足够时返回 {@code true}
     */
    public boolean lacksRestingCapacity(byte side, long price, long orderId) {
        if (!orders.canInsertWithoutResize(orderId)) return true;
        LongObjectHashMap<PriceLevel> levels = levelsFor(side);
        if (levels.get(price) != null) return false;
        LongHeap prices = pricesFor(side);
        if (!prices.hasCapacity()) {
            compactPrices(side);
        }
        return !(priceLevelTop > 0 && levels.canInsertWithoutResize(price) && prices.hasCapacity());
    }

    /**
     * 返回可用价格档节点数量。
     *
     * @return 可用价格档数量
     */
    public int availablePriceLevels() {
        return priceLevelTop;
    }

    /**
     * 从指定方向索引中返回最优价格档，同时移除价格堆中过期价格。
     *
     * @param levels 单边价格档映射
     * @param prices 单边最优价格堆
     * @return 最优非空价格档；该方向为空时返回 {@code null}
     */
    private PriceLevel bestLevel(LongObjectHashMap<PriceLevel> levels, LongHeap prices) {
        while (!prices.isEmpty()) {
            long p = prices.peek();
            PriceLevel level = levels.get(p);
            if (level != null && !level.isEmpty()) return level;
            prices.poll();
        }
        return null;
    }

    private boolean levelHasFillableQuantity(PriceLevel level, long requiredQty,
                                             long takerUserId, boolean preventSelfTrade) {
        long remaining = requiredQty;
        for (Order order = level.head; order != null; order = order.next) {
            if (preventSelfTrade && order.userId == takerUserId) return false;
            remaining -= order.remaining;
            if (remaining <= 0) return true;
        }
        return false;
    }

    private boolean levelHasSelfTradeBeforeFilled(PriceLevel level, long takerUserId, long quantity) {
        long remaining = quantity;
        for (Order order = level.head; order != null; order = order.next) {
            if (order.userId == takerUserId) return true;
            remaining -= order.remaining;
            if (remaining <= 0) return false;
        }
        return false;
    }

    private PriceLevel bestOppositeLevelFor(byte takerSide) {
        return takerSide == Side.BUY.code ? bestAskLevel() : bestBidLevel();
    }

    /**
     * 返回指定方向的价格档映射。
     *
     * @param side 方向编码
     * @return 对应方向的价格档映射
     */
    private LongObjectHashMap<PriceLevel> levelsFor(byte side) {
        return side == Side.BUY.code ? bids : asks;
    }

    /**
     * 返回吃单方对应的对手方价格档映射。
     *
     * @param takerSide 吃单方方向编码
     * @return 对手方价格档映射
     */
    private LongObjectHashMap<PriceLevel> oppositeLevelsFor(byte takerSide) {
        return takerSide == Side.BUY.code ? asks : bids;
    }

    /**
     * 返回吃单方对应的对手方价格堆。
     *
     * @param takerSide 吃单方方向编码
     * @return 对手方价格堆
     */
    private LongHeap oppositePricesFor(byte takerSide) {
        return takerSide == Side.BUY.code ? askPrices : bidPrices;
    }

    /**
     * 返回指定挂单方向的价格堆。
     *
     * @param side 挂单方向编码
     * @return 价格堆
     */
    private LongHeap pricesFor(byte side) {
        return side == Side.BUY.code ? bidPrices : askPrices;
    }

    /**
     * 重建指定方向的价格堆，清除懒删除残留价格。
     *
     * @param side 方向编码
     */
    private void compactPrices(byte side) {
        LongHeap prices = pricesFor(side);
        prices.clear();
        levelsFor(side).forEachValue(level -> {
            if (!level.isEmpty() && !prices.hasCapacity()) {
                throw new IllegalStateException("price heap capacity exhausted while compacting");
            }
            if (!level.isEmpty()) {
                prices.add(level.price);
            }
        });
    }

    /**
     * 返回吃单方对应的可复用扫描堆。
     *
     * @param takerSide 吃单方方向编码
     * @return 扫描堆
     */
    private LongHeap oppositeScanPricesFor(byte takerSide) {
        return takerSide == Side.BUY.code ? askScanPrices : bidScanPrices;
    }

    /**
     * 从池中借出价格档节点并绑定价格。
     *
     * @param price 价格
     * @return 价格档节点
     */
    private PriceLevel borrowPriceLevel(long price) {
        PriceLevel level = priceLevelPool[--priceLevelTop];
        priceLevelPool[priceLevelTop] = null;
        level.reset(price);
        return level;
    }

    /**
     * 归还空价格档节点。
     *
     * @param level 待归还节点
     */
    private void releasePriceLevel(PriceLevel level) {
        level.reset(0);
        priceLevelPool[priceLevelTop++] = level;
    }

    /**
     * 检查吃单方价格是否穿透挂单价格。
     *
     * @param takerSide 吃单方方向编码
     * @param takerPrice 吃单方限价
     * @param makerPrice 挂单价格
     * @return 价格穿透时返回 {@code true}
     */
    private boolean crosses(byte takerSide, long takerPrice, long makerPrice) {
        return takerSide == Side.BUY.code ? takerPrice >= makerPrice : takerPrice <= makerPrice;
    }
}
