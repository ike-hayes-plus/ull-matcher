package io.github.ike.ullmatcher.runtime;

import java.util.Objects;

/**
 * 撮合循环配置。
 */
public final class MatchLoopConfig {
    private final IdleStrategy idleStrategy;
    private final int maxBatchSize;

    /**
     * 创建配置。
     *
     * @param idleStrategy 空闲退避策略
     * @param maxBatchSize 单次批量处理上限
     */
    public MatchLoopConfig(IdleStrategy idleStrategy, int maxBatchSize) {
        this.idleStrategy = Objects.requireNonNull(idleStrategy, "idleStrategy");
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * 默认配置。
     *
     * @return 默认配置
     */
    public static MatchLoopConfig defaults() {
        return new MatchLoopConfig(AdaptiveIdleStrategy.defaults(), 64);
    }

    public IdleStrategy idleStrategy() {
        return idleStrategy;
    }

    public int maxBatchSize() {
        return maxBatchSize;
    }
}
