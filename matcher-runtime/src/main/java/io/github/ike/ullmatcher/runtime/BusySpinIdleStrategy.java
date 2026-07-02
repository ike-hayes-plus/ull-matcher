package io.github.ike.ullmatcher.runtime;

/**
 * 极致低延迟场景使用的纯自旋策略。
 */
public final class BusySpinIdleStrategy implements IdleStrategy {
    /** 单例。 */
    public static final BusySpinIdleStrategy INSTANCE = new BusySpinIdleStrategy();

    private volatile long spinCount;

    private BusySpinIdleStrategy() {}

    @Override
    public void idle(long consecutiveIdleCount) {
        spinCount++;
        Thread.onSpinWait();
    }

    @Override
    public long spinCount() {
        return spinCount;
    }
}
