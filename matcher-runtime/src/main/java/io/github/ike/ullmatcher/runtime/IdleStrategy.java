package io.github.ike.ullmatcher.runtime;

/**
 * 撮合线程空闲退避策略。
 */
public interface IdleStrategy {
    /**
     * 在一次空轮询后执行退避。
     *
     * @param consecutiveIdleCount 连续空轮询次数，从 1 开始
     */
    void idle(long consecutiveIdleCount);

    /**
     * 在收到命令后重置退避状态。
     */
    default void reset() {}

    /**
     * 返回累计空转次数。
     *
     * @return 累计空转次数
     */
    default long spinCount() {
        return 0L;
    }

    /**
     * 返回累计让出 CPU 次数。
     *
     * @return 累计让出 CPU 次数
     */
    default long yieldCount() {
        return 0L;
    }

    /**
     * 返回累计短暂挂起次数。
     *
     * @return 累计挂起次数
     */
    default long parkCount() {
        return 0L;
    }
}
