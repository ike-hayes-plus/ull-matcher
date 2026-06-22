package io.github.ike.ullmatcher.api;

/**
 * TTL 守护线程向上游发出的结构化事件。
 */
public final class TtlEvent {
    /** 事件发生时间，单位为 epoch millis。 */
    public long eventTimeEpochMillis;

    /** 订单所属的交易对或分片编号。 */
    public int symbolId;

    /** 受影响订单编号。 */
    public long orderId;

    /** 触发该事件的动作类型。 */
    public TtlEventAction action;

    /** 当前动作的来源。 */
    public String source;

    /** 当前动作的结果或原因。 */
    public String detail;

    /** 订单绝对过期时间，单位为 epoch millis。 */
    public long expireAtEpochMillis;

    /**
     * 创建空事件。
     */
    public TtlEvent() {}
}
