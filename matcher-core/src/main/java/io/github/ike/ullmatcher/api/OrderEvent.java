package io.github.ike.ullmatcher.api;

/** 面向订单服务和客户端的订单状态变更事件。 */
public final class OrderEvent {
    /** 产生该事件的命令序列号。 */
    public long sequence;

    /** 受影响订单所属的交易对或分片编号。 */
    public int symbolId;

    /** 受影响的订单编号。 */
    public long orderId;

    /** 命令或成交处理后的当前订单状态。 */
    public OrderStatus status;

    /** 拒绝原因；非拒绝事件使用 {@link RejectReason#NONE}。 */
    public RejectReason rejectReason = RejectReason.NONE;

    /** 该事件后的剩余未成交数量。 */
    public long remaining;

    /** 订单绝对过期时间，单位为 epoch millis；非 TTL 订单或终态事件可为 {@code 0}。 */
    public long expireAtEpochMillis;

    /**
     * 创建空的可复用订单事件。
     */
    public OrderEvent() {}

    /**
     * 返回紧凑的诊断字符串。
     *
     * @return 用于日志和示例的事件详情
     */
    @Override
    public String toString() {
        return "OrderEvent{" + "seq=" + sequence + ", orderId=" + orderId + ", status=" + status +
                ", remaining=" + remaining + ", expireAt=" + expireAtEpochMillis + ", reject=" + rejectReason + '}';
    }
}
