package io.github.ike.ullmatcher.api;

/** 订单买卖方向。热路径内部使用字节常量，避免枚举分支开销。 */
public enum Side {
    /** 买方。 */
    BUY((byte) 1),

    /** 卖方。 */
    SELL((byte) 2);

    /** 买卖方向在线路格式和热路径中的紧凑编码。 */
    public final byte code;

    /**
     * 使用字节编码创建买卖方向。
     *
     * @param code 线路格式和热路径中的紧凑编码
     */
    Side(byte code) {
        this.code = code;
    }

    /**
     * 解码字节形式的买卖方向。
     *
     * @param code 买卖方向紧凑编码
     * @return 买方编码返回 {@link #BUY}，否则返回 {@link #SELL}
     */
    public static Side from(byte code) {
        return code == BUY.code ? BUY : SELL;
    }
}
