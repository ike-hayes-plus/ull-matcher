package io.github.ike.ullmatcher.ha.snapshot;

import java.nio.file.Path;
import java.util.Objects;

/**
 * 可同步的本地快照物料。
 *
 * @param file 快照文件
 * @param lastSequence 快照覆盖的最后命令序列号
 * @param lastTradeId 快照覆盖的最后成交编号
 * @param liveOrderCount 快照中的活跃挂单数量
 */
public record SnapshotMaterial(
        Path file,
        long lastSequence,
        long lastTradeId,
        long liveOrderCount
) {
    public SnapshotMaterial {
        Objects.requireNonNull(file, "file");
        if (lastSequence < 0L || lastTradeId < 0L || liveOrderCount < 0L) {
            throw new IllegalArgumentException("snapshot metadata values must be non-negative");
        }
    }
}
