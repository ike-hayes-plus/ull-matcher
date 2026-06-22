package io.github.ike.ullmatcher.api;

/** 撮合核心支持的订单有效期策略。 */
public enum TimeInForce {
    /** 成交后仍可挂单的长期有效订单。 */
    GTC((byte) 1),

    /** 立即成交否则撤销未成交剩余数量的订单。 */
    IOC((byte) 2),

    /** 撮合前必须确认可完全成交的订单。 */
    FOK((byte) 3),

    /** 只做挂单方的订单；若会立即吃单则拒绝。 */
    POST_ONLY((byte) 4);

    /** 有效期策略在线路格式和热路径中的紧凑编码。 */
    public final byte code;

    /**
     * 使用字节编码创建有效期策略。
     *
     * @param code 线路格式和热路径中的紧凑编码
     */
    TimeInForce(byte code) {
        this.code = code;
    }

    /**
     * 解码字节形式的有效期策略。
     *
     * @param code 有效期策略紧凑编码
     * @return 解码后的策略，默认返回 {@link #GTC}
     */
    public static TimeInForce from(byte code) {
        if (code == IOC.code) return IOC;
        if (code == FOK.code) return FOK;
        if (code == POST_ONLY.code) return POST_ONLY;
        return GTC;
    }
}
