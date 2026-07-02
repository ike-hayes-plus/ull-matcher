package io.github.ike.ullmatcher.runtime;

/**
 * 撮合循环可观测快照。
 */
public record MatchLoopSnapshot(
        MatchLoopState state,
        boolean acceptingCommands,
        long processedCommandCount,
        long idlePollCount,
        long idleSpinCount,
        long idleYieldCount,
        long idleParkCount,
        long lastBatchSize,
        long maxBatchSizeObserved,
        long lastCommandNanos,
        Throwable failure
) {}
