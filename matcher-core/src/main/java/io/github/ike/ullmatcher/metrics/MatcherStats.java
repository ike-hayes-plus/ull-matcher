package io.github.ike.ullmatcher.metrics;

/**
 * 撮合器运行指标快照。
 * <p>
 * 该对象只在调用 {@code UltraLowLatencyMatcher.stats()} 时创建，不进入撮合热路径。
 *
 * @param lastSequence 最后已应用命令序列号
 * @param lastTradeId 最后成交编号
 * @param liveOrderCount 当前活跃挂单数量
 * @param commandCount 已接收命令数量
 * @param tradeCount 已生成成交数量
 * @param orderEventCount 已生成订单事件数量
 * @param rejectedCommandCount 已拒绝命令数量
 * @param capacityRejectedCommandCount 容量不足拒绝数量
 * @param orderPoolAvailable 当前可用订单对象数量
 * @param orderPoolExhaustedCount 订单对象池耗尽次数
 * @param priceLevelPoolAvailable 当前可用价格档对象数量
 */
public record MatcherStats(long lastSequence,
                           long lastTradeId,
                           long liveOrderCount,
                           long commandCount,
                           long tradeCount,
                           long orderEventCount,
                           long rejectedCommandCount,
                           long capacityRejectedCommandCount,
                           int orderPoolAvailable,
                           long orderPoolExhaustedCount,
                           int priceLevelPoolAvailable) {}
