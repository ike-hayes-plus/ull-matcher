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

    /** 已入簿订单被撤销，或 IOC 未成交剩余被立即撤销。 */
    CANCELLED,

    /** 订单未入簿且未产生成交即被拒绝，含 FOK 不可全成与容量拒绝。 */
    REJECTED
}
