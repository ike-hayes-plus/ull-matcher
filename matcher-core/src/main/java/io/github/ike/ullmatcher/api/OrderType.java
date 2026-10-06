package io.github.ike.ullmatcher.api;

/**
 * 撮合核心支持的订单类型。
 * <p>
 * 市价风格订单必须携带有界保护价，以便撮合器仍然执行确定性的限价穿透检查。
 */
public enum OrderType {
    /** 可挂单或吃单的限价订单。 */
    LIMIT((byte) 1),

    /** 带显式保护价的市价风格订单。 */
    MARKET_WITH_PROTECTION((byte) 2);

    /** 订单类型在线路格式和热路径中的紧凑编码。 */
    public final byte code;

    /**
     * 使用字节编码创建订单类型。
     *
     * @param code 线路格式和热路径中的紧凑编码
     */
    OrderType(byte code) {
        this.code = code;
    }

    /**
     * 解码字节形式的订单类型。
     *
     * @param code 订单类型紧凑编码
     * @return 匹配的订单类型
     * @throws IllegalArgumentException 编码不是已声明的订单类型
     */
    public static OrderType from(byte code) {
        if (code == LIMIT.code) {
            return LIMIT;
        }
        if (code == MARKET_WITH_PROTECTION.code) {
            return MARKET_WITH_PROTECTION;
        }
        throw new IllegalArgumentException("unknown orderType code: " + code);
    }
}
