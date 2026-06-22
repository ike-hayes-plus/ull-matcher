package io.github.ike.ullmatcher.ha.snapshot;

import java.nio.file.Path;
import java.util.Objects;

/**
 * 远端快照下载结果。
 *
 * @param file 已落地的本地快照路径
 * @param bytesWritten 实际写入字节数
 * @param lastSequence 快照覆盖的最后命令序列号
 * @param lastTradeId 快照覆盖的最后成交编号
 * @param liveOrderCount 快照中的活跃挂单数量
 */
public record SnapshotSyncResult(
        Path file,
        long bytesWritten,
        long lastSequence,
        long lastTradeId,
        long liveOrderCount
) {
    public SnapshotSyncResult {
        Objects.requireNonNull(file, "file");
        if (bytesWritten < 0L || lastSequence < 0L || lastTradeId < 0L || liveOrderCount < 0L) {
            throw new IllegalArgumentException("snapshot sync values must be non-negative");
        }
    }
}
