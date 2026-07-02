package io.github.ike.ullmatcher.ha.replication;

import java.util.List;

/**
 * 一次复制尝试的结果。
 *
 * @param totalTargets 总 standby 数
 * @param ackedTargets 成功确认数
 * @param ackedNodeIds 已确认节点
 * @param failedNodeIds 失败节点
 */
public record ReplicationResult(
        int totalTargets,
        int ackedTargets,
        List<String> ackedNodeIds,
        List<String> failedNodeIds
) {
    public ReplicationResult {
        ackedNodeIds = List.copyOf(ackedNodeIds);
        failedNodeIds = List.copyOf(failedNodeIds);
    }

    public boolean satisfies(ReplicationMode mode) {
        return ackedTargets >= mode.requiredAcks(totalTargets);
    }
}
