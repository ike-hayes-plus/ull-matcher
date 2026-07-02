package io.github.ike.ullmatcher.api;

/** 成交事件。清结算服务消费该事件；撮合器自身不修改资产余额。 */
public final class TradeEvent {
    /** 产生该成交的命令序列号。 */
    public long sequence;

    /** 撮合器本地单调递增的成交编号。 */
    public long tradeId;

    /** 成交所属的交易对或分片编号。 */
    public int symbolId;

    /** 买单编号。 */
    public long buyOrderId;

    /** 卖单编号。 */
    public long sellOrderId;

    /** 买方用户编号。 */
    public long buyerUserId;

    /** 卖方用户编号。 */
    public long sellerUserId;

    /** 定点数成交价格。 */
    public long price;

    /** 定点数成交数量。 */
    public long quantity;

    /** 按 {@code price * quantity / quoteScale} 计算的计价金额。 */
    public long quoteAmount;

    /**
     * 创建空的可复用成交事件。
     */
    public TradeEvent() {}

    /**
     * 返回紧凑的诊断字符串。
     *
     * @return 用于日志和示例的成交详情
     */
    @Override
    public String toString() {
        return "TradeEvent{" + "seq=" + sequence + ", tradeId=" + tradeId + ", price=" + price +
                ", qty=" + quantity + ", buy=" + buyOrderId + ", sell=" + sellOrderId + '}';
    }
}
