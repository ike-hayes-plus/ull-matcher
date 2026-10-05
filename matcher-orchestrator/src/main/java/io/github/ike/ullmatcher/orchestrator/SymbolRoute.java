package io.github.ike.ullmatcher.orchestrator;

import java.util.Objects;
import java.util.Optional;

/**
 * {@code symbolId} 到分片及端点的解析结果。
 */
public record SymbolRoute(
        int symbolId,
        String shardKey,
        long generation,
        long updatedAtEpochMillis,
        RegisteredShard shard
) {
    public SymbolRoute {
        if (symbolId <= 0) {
            throw new IllegalArgumentException("symbolId must be positive");
        }
        Objects.requireNonNull(shardKey, "shardKey");
        if (shardKey.isBlank()) {
            throw new IllegalArgumentException("shardKey must not be blank");
        }
        if (generation < 0L || updatedAtEpochMillis < 0L) {
            throw new IllegalArgumentException("generation and updatedAtEpochMillis must be non-negative");
        }
    }

    public Optional<RegisteredShard> activeShard() {
        if (shard == null || shard.state() != ShardLifecycleState.ACTIVE) {
            return Optional.empty();
        }
        return Optional.of(shard);
    }
}
