package io.github.ike.ullmatcher.api;

/**
 * 撮合线程内调用的回调接口，实现不得阻塞。
 * <p>
 * 建议做法：将字段复制到出站环形缓冲区后立即返回。
 */
public interface MatchEventHandler {
    /**
     * 处理成交事件。
     *
     * @param event 撮合器填充的可复用成交事件对象
     */
    void onTrade(TradeEvent event);

    /**
     * 处理订单状态事件。
     *
     * @param event 撮合器填充的可复用订单事件对象
     */
    void onOrder(OrderEvent event);

    /**
     * 处理 TTL 生命周期事件。
     *
     * @param event TTL 守护线程发出的结构化事件
     */
    default void onTtl(TtlEvent event) {
    }
}
