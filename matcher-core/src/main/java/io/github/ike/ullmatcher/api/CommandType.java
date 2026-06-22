package io.github.ike.ullmatcher.api;

/**
 * 撮合分片接受的命令类型。
 */
public enum CommandType {
    /** 提交新订单，进入订单簿或撮合路径。 */
    NEW_ORDER,

    /** 撤销已有挂单。 */
    CANCEL_ORDER,

    /** 发出快照编排使用的安全点标记。 */
    SNAPSHOT_MARKER,

    /** 停止所属撮合循环。 */
    SHUTDOWN
}
