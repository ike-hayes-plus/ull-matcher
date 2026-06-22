package io.github.ike.ullmatcher.storage.wal;

import java.nio.file.Path;
import java.util.List;

/**
 * WAL 分段清理结果。
 *
 * @param snapshotSequence 本次清理依据的快照序列号
 * @param archivedSegments 已归档的分段路径
 * @param deletedSegments 已删除的本地分段路径
 */
public record WalRetentionResult(long snapshotSequence, List<Path> archivedSegments, List<Path> deletedSegments) {
    /**
     * 创建不可变清理结果。
     *
     * @param snapshotSequence 本次清理依据的快照序列号
     * @param archivedSegments 已归档的分段路径
     * @param deletedSegments 已删除的本地分段路径
     */
    public WalRetentionResult {
        archivedSegments = List.copyOf(archivedSegments);
        deletedSegments = List.copyOf(deletedSegments);
    }
}
