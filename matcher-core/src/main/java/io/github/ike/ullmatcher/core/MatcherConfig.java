package io.github.ike.ullmatcher.core;

/**
 * 引擎容量配置项。生产环境应积极预分配，避免运行中扩容。
 *
 * @param symbolId            当前撮合器实例处理的交易对或分片编号。
 * @param expectedPriceLevels 预期活跃价格档数量，用于设置价格映射和堆容量。
 * @param expectedLiveOrders  预期活跃订单数量，用于设置订单索引容量。
 * @param orderPoolSize       对象池中预分配的订单节点数量。
 * @param quoteScale          计算计价金额使用的定点数报价比例。
 * @param preventSelfTrade    是否拒绝可能与同一用户成交的订单。
 */
public record MatcherConfig(int symbolId, int expectedPriceLevels, int expectedLiveOrders, int orderPoolSize,
                            long quoteScale, boolean preventSelfTrade) {
    /**
     * 创建撮合器配置。
     *
     * @param symbolId            交易对或分片编号
     * @param expectedPriceLevels 预期活跃价格档数量
     * @param expectedLiveOrders  预期活跃订单数量
     * @param orderPoolSize       预分配订单节点数量
     * @param quoteScale          报价定点数比例
     * @param preventSelfTrade    是否拒绝同用户穿透成交
     */
    public MatcherConfig {
        if (symbolId <= 0) {
            throw new IllegalArgumentException("symbolId must be positive");
        }
        if (expectedPriceLevels <= 0 || expectedLiveOrders <= 0 || orderPoolSize <= 0) {
            throw new IllegalArgumentException("capacity settings must be positive");
        }
        if (quoteScale <= 0) {
            throw new IllegalArgumentException("quoteScale must be positive");
        }
    }

    /**
     * 返回单交易对的生产向默认容量配置。
     *
     * @param symbolId 交易对或分片编号
     * @return 默认撮合器配置
     */
    public static MatcherConfig defaults(int symbolId) {
        return new MatcherConfig(symbolId, 1 << 16, 1 << 20, 1 << 20, 100_000_000L, true);
    }
}
