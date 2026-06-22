package io.github.ike.ullmatcher.api;

/**
 * 订单生命周期状态。
 */
public enum OrderStatus {
    /** 订单已接受并挂在订单簿。 */
    NEW,

    /** 订单部分成交且仍有剩余数量。 */
    PARTIALLY_FILLED,

    /** 订单数量已全部成交。 */
    FILLED,

    /** 订单或未成交剩余数量已撤销。 */
    CANCELLED,

    /** 订单在修改订单簿前被拒绝。 */
    REJECTED
}
