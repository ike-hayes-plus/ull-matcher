package io.github.ike.ullmatcher.api;

/** 撮合引擎输出的轻量拒绝原因。 */
public enum RejectReason {
    /** 未发生拒绝。 */
    NONE,

    /** 命令序列号没有大于最后已应用序列号。 */
    DUPLICATE_SEQUENCE,

    /** 新订单复用了仍然活跃的订单编号。 */
    DUPLICATE_ORDER_ID,

    /** 命令包含非法交易对、方向、价格或数量。 */
    INVALID_ORDER,

    /** 撤单命令指向了不存在的活跃订单。 */
    UNKNOWN_ORDER,

    /** 只挂单订单会穿透订单簿并吃单。 */
    POST_ONLY_WOULD_TAKE,

    /** 现有对手方流动性无法完全成交 FOK 订单。 */
    FOK_NOT_FILLABLE,

    /** 订单被防自成交逻辑拒绝。 */
    SELF_TRADE_PREVENTED,

    /** 撮合器预分配容量已耗尽。 */
    CAPACITY_EXCEEDED
}
