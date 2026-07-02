package io.github.ike.ullmatcher.server.engine;

/**
 * 提交请求在服务端的生命周期阶段。
 */
public enum SubmissionPhase {
    /**
     * 请求已经登记，但尚未完成本地 WAL 落盘。
     */
    RECEIVED,

    /**
     * 请求已经进入本地 WAL，正在等待跨节点复制确认。
     */
    REPLICATION_PENDING,

    /**
     * 请求已经达到当前复制策略要求的确认水位。
     */
    COMMITTED,

    /**
     * 请求在进入本地 WAL 前失败，或进入本地 WAL 后进入恢复/人工处置状态。
     */
    FAILED
}
