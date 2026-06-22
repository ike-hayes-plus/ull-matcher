package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.coordination.HaTickResult;
import io.github.ike.ullmatcher.ha.readiness.StandbyReadinessGate;

import java.util.List;
import java.util.Map;

/**
 * 汇总集群监督循环的运行指标与最近一次传输状态。
 */
public record ClusterSupervisorMetricsSnapshot(
        long tickCount,
        long tickFailureCount,
        HaTickResult lastTickResult,
        StandbyReadinessGate.GateDecision lastGateDecision,
        Map<String, Long> errorCounts,
        List<String> recentErrors,
        String syncState,
        String lastSyncError,
        TransportMetricsSnapshot transportMetrics
) {
}
