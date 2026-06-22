package io.github.ike.ullmatcher.runtime;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandType;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * 专用平台线程撮合循环。不要在虚拟线程上运行。
 */
public final class MatchLoop implements Runnable {
    private final SpscRingBuffer<Command> input;
    private final UltraLowLatencyMatcher matcher;
    private final BiConsumer<Command, Throwable> failureHandler;
    private final MatchLoopConfig config;
    private final Command[] batchBuffer;

    private volatile boolean running = true;
    private volatile boolean acceptingCommands = true;
    private volatile boolean stopWhenDrained;
    private volatile Throwable failure;
    private volatile MatchLoopState state = MatchLoopState.STARTING;
    private volatile long processedCommandCount;
    private volatile long idlePollCount;
    private volatile long lastBatchSize;
    private volatile long maxBatchSizeObserved;
    private volatile long lastCommandNanos;

    /**
     * 创建撮合循环。
     *
     * @param input 入站命令环形缓冲区
     * @param matcher 当前循环持有的撮合器实例
     */
    public MatchLoop(SpscRingBuffer<Command> input, UltraLowLatencyMatcher matcher) {
        this(input, matcher, MatchLoopConfig.defaults(), (command, error) -> {});
    }

    /**
     * 创建带失败回调的撮合循环。
     *
     * @param input 入站命令环形缓冲区
     * @param matcher 当前循环持有的撮合器实例
     * @param failureHandler 命令处理失败回调
     */
    public MatchLoop(SpscRingBuffer<Command> input, UltraLowLatencyMatcher matcher,
                     BiConsumer<Command, Throwable> failureHandler) {
        this(input, matcher, MatchLoopConfig.defaults(), failureHandler);
    }

    /**
     * 创建带配置和失败回调的撮合循环。
     *
     * @param input 入站命令环形缓冲区
     * @param matcher 当前循环持有的撮合器实例
     * @param config 循环配置
     * @param failureHandler 命令处理失败回调
     */
    public MatchLoop(SpscRingBuffer<Command> input, UltraLowLatencyMatcher matcher,
                     MatchLoopConfig config, BiConsumer<Command, Throwable> failureHandler) {
        this.input = Objects.requireNonNull(input, "input");
        this.matcher = Objects.requireNonNull(matcher, "matcher");
        this.config = Objects.requireNonNull(config, "config");
        this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
        this.batchBuffer = new Command[config.maxBatchSize()];
    }

    @Override
    public void run() {
        long consecutiveIdleCount = 0L;
        state = MatchLoopState.RUNNING;
        while (running) {
            int drained = input.drain(batchBuffer, config.maxBatchSize());
            if (drained == 0) {
                if (stopWhenDrained) {
                    acceptingCommands = false;
                    running = false;
                    state = MatchLoopState.STOPPED;
                    continue;
                }
                idlePollCount++;
                consecutiveIdleCount++;
                state = acceptingCommands ? MatchLoopState.RUNNING : MatchLoopState.QUIESCING;
                config.idleStrategy().idle(consecutiveIdleCount);
                continue;
            }

            config.idleStrategy().reset();
            consecutiveIdleCount = 0L;
            long processedInBatch = 0L;
            int consumed = 0;
            try {
                while (consumed < drained) {
                    Command command = batchBuffer[consumed];
                    batchBuffer[consumed] = null;
                    process(command);
                    consumed++;
                    processedInBatch++;
                    if (!running) {
                        break;
                    }
                }
            } finally {
                while (consumed < drained) {
                    Command buffered = batchBuffer[consumed];
                    batchBuffer[consumed] = null;
                    if (buffered != null) {
                        buffered.release();
                    }
                    consumed++;
                }
            }

            lastBatchSize = processedInBatch;
            if (processedInBatch > maxBatchSizeObserved) {
                maxBatchSizeObserved = processedInBatch;
            }
            if (running) {
                state = acceptingCommands ? MatchLoopState.RUNNING
                        : (stopWhenDrained ? MatchLoopState.DRAINING : MatchLoopState.QUIESCING);
            }
        }
        if (failure == null && state != MatchLoopState.STOPPED) {
            state = MatchLoopState.STOPPED;
        }
    }

    /**
     * 停止接收新命令，但继续处理已经入队的命令。
     */
    public void quiesce() {
        acceptingCommands = false;
        if (running) {
            state = MatchLoopState.QUIESCING;
        }
    }

    /**
     * 重新进入可接单状态。
     * <p>
     * 仅适用于仍在运行、未失败、未进入 drain 停机流程的循环。
     */
    public void activate() {
        if (!running) {
            throw new IllegalStateException("cannot activate stopped loop");
        }
        if (failure != null) {
            throw new IllegalStateException("cannot activate failed loop");
        }
        if (stopWhenDrained) {
            throw new IllegalStateException("cannot activate draining loop");
        }
        acceptingCommands = true;
        state = MatchLoopState.RUNNING;
    }

    /**
     * 停止接单，并在队列排空后退出循环。
     */
    public void drainAndStop() {
        acceptingCommands = false;
        stopWhenDrained = true;
        if (running) {
            state = MatchLoopState.DRAINING;
        }
    }

    /**
     * 请求立即停止循环。
     */
    public void stop() {
        acceptingCommands = false;
        running = false;
        if (failure == null) {
            state = MatchLoopState.STOPPED;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isAcceptingCommands() {
        return running && acceptingCommands && failure == null;
    }

    public Throwable failure() {
        return failure;
    }

    public MatchLoopState state() {
        return state;
    }

    public long processedCommandCount() {
        return processedCommandCount;
    }

    public long idleSpinCount() {
        return config.idleStrategy().spinCount();
    }

    public long idlePollCount() {
        return idlePollCount;
    }

    public long idleYieldCount() {
        return config.idleStrategy().yieldCount();
    }

    public long idleParkCount() {
        return config.idleStrategy().parkCount();
    }

    public MatchLoopSnapshot snapshot() {
        return new MatchLoopSnapshot(
                state,
                isAcceptingCommands(),
                processedCommandCount,
                idlePollCount,
                idleSpinCount(),
                idleYieldCount(),
                idleParkCount(),
                lastBatchSize,
                maxBatchSizeObserved,
                lastCommandNanos,
                failure
        );
    }

    private void process(Command command) {
        try {
            if (command.type == CommandType.SHUTDOWN) {
                acceptingCommands = false;
                running = false;
                state = MatchLoopState.STOPPED;
                return;
            }
            matcher.onCommand(command);
            processedCommandCount++;
            lastCommandNanos = System.nanoTime();
        } catch (Throwable t) {
            failure = t;
            acceptingCommands = false;
            running = false;
            state = MatchLoopState.FAILED;
            try {
                failureHandler.accept(command, t);
            } catch (Throwable handlerError) {
                t.addSuppressed(handlerError);
            }
        } finally {
            command.release();
        }
    }
}
