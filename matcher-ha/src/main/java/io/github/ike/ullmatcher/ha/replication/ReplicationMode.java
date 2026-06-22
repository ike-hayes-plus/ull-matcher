package io.github.ike.ullmatcher.ha.replication;

/**
 * WAL 复制确认策略。
 */
public enum ReplicationMode {
    LOCAL_ONLY,
    WAIT_FOR_ANY_STANDBY,
    WAIT_FOR_QUORUM_STANDBYS,
    WAIT_FOR_ALL_STANDBYS;

    /**
     * 计算当前策略要求的 standby 确认数。
     */
    public int requiredAcks(int standbyCount) {
        return switch (this) {
            case LOCAL_ONLY -> 0;
            case WAIT_FOR_ANY_STANDBY -> Math.min(1, standbyCount);
            case WAIT_FOR_QUORUM_STANDBYS -> standbyCount == 0 ? 0 : (standbyCount / 2) + 1;
            case WAIT_FOR_ALL_STANDBYS -> standbyCount;
        };
    }
}
