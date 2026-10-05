package io.github.ike.ullmatcher.orchestrator;

import java.util.Objects;

/**
 * 控制面注册的一个分片实例（一进程一分片模型）。
 */
public record RegisteredShard(
        String shardKey,
        int symbolId,
        String nodeId,
        ShardEndpoints endpoints,
        ShardLifecycleState state,
        long generation,
        long updatedAtEpochMillis
) {
    public RegisteredShard {
        Objects.requireNonNull(shardKey, "shardKey");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(endpoints, "endpoints");
        Objects.requireNonNull(state, "state");
        if (shardKey.isBlank() || nodeId.isBlank()) {
            throw new IllegalArgumentException("shardKey and nodeId must not be blank");
        }
        if (symbolId <= 0) {
            throw new IllegalArgumentException("symbolId must be positive");
        }
        if (generation < 0L || updatedAtEpochMillis < 0L) {
            throw new IllegalArgumentException("generation and updatedAtEpochMillis must be non-negative");
        }
    }

    public RegisteredShard withState(ShardLifecycleState newState, long newUpdatedAtEpochMillis) {
        return new RegisteredShard(
                shardKey,
                symbolId,
                nodeId,
                endpoints,
                newState,
                generation,
                newUpdatedAtEpochMillis
        );
    }
}
