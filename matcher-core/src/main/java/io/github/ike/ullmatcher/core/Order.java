package io.github.ike.ullmatcher.core;

/** 仅在单个撮合线程内保存的可变订单节点。按设计不是线程安全的。 */
public final class Order {
    /** 唯一订单编号。 */
    public long orderId;

    /** 归属用户编号。 */
    public long userId;

    /** 交易对或分片编号。 */
    public int symbolId;

    /** 来自 {@code Side.code} 的方向编码。 */
    public byte side;

    /** 来自 {@code TimeInForce.code} 的有效期策略编码。 */
    public byte timeInForce;

    /** 定点数限价或保护价。 */
    public long price;

    /** 原始定点数数量。 */
    public long quantity;

    /** 剩余未成交定点数数量。 */
    public long remaining;

    /** 创建该订单的命令序列号。 */
    public long sequence;

    /** 订单绝对过期时间，单位为 epoch millis；非 TTL 订单为 {@code 0}。 */
    public long expireAtEpochMillis;

    /** 同一价格档 FIFO 队列中的前一个订单。 */
    public Order prev;

    /** 同一价格档 FIFO 队列中的后一个订单。 */
    public Order next;

    /**
     * 创建归对象池管理的空订单节点。
     */
    public Order() {}

    /**
     * 在节点归还对象池前清空全部可变状态。
     */
    public void reset() {
        orderId = userId = price = quantity = remaining = sequence = expireAtEpochMillis = 0;
        symbolId = 0; side = 0; timeInForce = 0; prev = null; next = null;
    }
}
