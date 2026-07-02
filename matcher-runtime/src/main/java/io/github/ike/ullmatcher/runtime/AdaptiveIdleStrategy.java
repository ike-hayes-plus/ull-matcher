package io.github.ike.ullmatcher.runtime;

import java.util.concurrent.locks.LockSupport;

/**
 * 自适应空闲策略。
 * <p>
 * 高频流量下先短暂自旋，随后让出 CPU；长时间无流量时改为纳秒级 park，
 * 避免冷门分片、备机或夜间时段长期烧满一个核。
 */
public final class AdaptiveIdleStrategy implements IdleStrategy {
    private final int spinTries;
    private final int yieldTries;
    private final long initialParkNanos;
    private final long maxParkNanos;

    private volatile long spinCount;
    private volatile long yieldCount;
    private volatile long parkCount;
    private volatile long currentParkNanos;

    /**
     * 创建自适应策略。
     *
     * @param spinTries 纯自旋次数
     * @param yieldTries 让出 CPU 次数
     * @param initialParkNanos 初始挂起时间
     * @param maxParkNanos 最大挂起时间
     */
    public AdaptiveIdleStrategy(int spinTries, int yieldTries, long initialParkNanos, long maxParkNanos) {
        if (spinTries < 0) {
            throw new IllegalArgumentException("spinTries must be non-negative");
        }
        if (yieldTries < 0) {
            throw new IllegalArgumentException("yieldTries must be non-negative");
        }
        if (initialParkNanos <= 0) {
            throw new IllegalArgumentException("initialParkNanos must be positive");
        }
        if (maxParkNanos < initialParkNanos) {
            throw new IllegalArgumentException("maxParkNanos must be >= initialParkNanos");
        }
        this.spinTries = spinTries;
        this.yieldTries = yieldTries;
        this.initialParkNanos = initialParkNanos;
        this.maxParkNanos = maxParkNanos;
        this.currentParkNanos = initialParkNanos;
    }

    /**
     * 生产默认策略。
     *
     * @return 默认自适应策略
     */
    public static AdaptiveIdleStrategy defaults() {
        return new AdaptiveIdleStrategy(64, 64, 1_000L, 250_000L);
    }

    @Override
    public void idle(long consecutiveIdleCount) {
        if (consecutiveIdleCount <= spinTries) {
            spinCount++;
            Thread.onSpinWait();
            return;
        }
        if (consecutiveIdleCount <= (long) spinTries + yieldTries) {
            yieldCount++;
            Thread.yield();
            return;
        }
        parkCount++;
        LockSupport.parkNanos(currentParkNanos);
        if (currentParkNanos < maxParkNanos) {
            currentParkNanos = Math.min(maxParkNanos, currentParkNanos << 1);
        }
    }

    @Override
    public void reset() {
        currentParkNanos = initialParkNanos;
    }

    @Override
    public long spinCount() {
        return spinCount;
    }

    @Override
    public long yieldCount() {
        return yieldCount;
    }

    @Override
    public long parkCount() {
        return parkCount;
    }
}
