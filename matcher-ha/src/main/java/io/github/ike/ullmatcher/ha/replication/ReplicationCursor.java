package io.github.ike.ullmatcher.ha.replication;

/**
 * 副本复制进度。
 *
 * @param lastReceivedSequence 最近接收的 WAL 序列号
 * @param lastDurableSequence 最近落盘的 WAL 序列号
 * @param lastAppliedSequence 最近完成重放的命令序列号
 * @param snapshotSequence 最近快照序列号
 */
public record ReplicationCursor(
        long lastReceivedSequence,
        long lastDurableSequence,
        long lastAppliedSequence,
        long snapshotSequence
) {
    public ReplicationCursor {
        if (lastReceivedSequence < 0L || lastDurableSequence < 0L || lastAppliedSequence < 0L || snapshotSequence < 0L) {
            throw new IllegalArgumentException("replication cursor values must be non-negative");
        }
    }

    public long promotionWatermark() {
        return Math.min(lastDurableSequence, lastAppliedSequence);
    }
}
