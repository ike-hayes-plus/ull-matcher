package io.github.ike.ullmatcher.core;

/** 单一价格档上的 FIFO 队列，用于保持时间优先。 */
public final class PriceLevel {
    /** 该价格档代表的价格。 */
    public long price;

    /** 时间优先级最高的首个订单。 */
    public Order head;

    /** 时间优先级最低的尾部订单。 */
    public Order tail;

    /** 该价格档内全部订单的剩余总数量。 */
    public long totalQuantity;

    /** 该价格档内的活跃订单数量。 */
    public int orderCount;

    /**
     * 创建未绑定价格的价格档节点。
     */
    public PriceLevel() {}

    /**
     * 将池化价格档重新绑定到指定价格。
     *
     * @param price 新价格
     */
    public void reset(long price) {
        this.price = price;
        head = null;
        tail = null;
        totalQuantity = 0;
        orderCount = 0;
    }

    /**
     * 将订单追加到 FIFO 队列尾部。
     *
     * @param order 待追加订单
     */
    public void addLast(Order order) {
        order.prev = tail; order.next = null;
        if (tail == null) head = order; else tail.next = order;
        tail = order;
        totalQuantity += order.remaining;
        orderCount++;
    }

    /**
     * 从当前价格档移除订单。
     *
     * @param order 当前已挂接在该价格档中的活跃订单
     */
    public void remove(Order order) {
        if (order.prev == null) head = order.next; else order.prev.next = order.next;
        if (order.next == null) tail = order.prev; else order.next.prev = order.prev;
        totalQuantity -= order.remaining;
        orderCount--;
        order.prev = null; order.next = null;
    }

    /**
     * 扣减该价格档内已经成交的数量。
     *
     * @param quantity 已成交数量
     */
    public void decreaseQuantity(long quantity) {
        if (quantity <= 0 || quantity > totalQuantity) {
            throw new IllegalArgumentException("invalid price level quantity decrease");
        }
        totalQuantity -= quantity;
    }

    /**
     * 判断该价格档是否没有活跃订单。
     *
     * @return 没有剩余订单时返回 {@code true}
     */
    public boolean isEmpty() {
        return orderCount == 0;
    }
}
