package io.github.ike.ullmatcher.api;

/**
 * TTL 守护线程对订单触发的生命周期动作。
 */
public enum TtlEventAction {
    /** 已为订单建立 TTL 跟踪。 */
    SCHEDULED,

    /** 订单到达过期时间。 */
    EXPIRED,

    /** 已提交 TTL 撤单命令。 */
    CANCEL_REQUESTED,

    /** TTL 撤单命令已被接收。 */
    CANCEL_ACCEPTED,

    /** TTL 撤单命令已写入 WAL，等待恢复链闭合。 */
    CANCEL_PENDING_RECOVERY,

    /** TTL 撤单命令未被接收，等待下一次重试。 */
    CANCEL_SKIPPED,

    /** TTL 撤单执行失败。 */
    CANCEL_FAILED,

    /** 订单被重新加入下一轮 TTL 扫描。 */
    RESCHEDULED,

    /** 订单已从 TTL 跟踪集合中移除。 */
    REMOVED
}
