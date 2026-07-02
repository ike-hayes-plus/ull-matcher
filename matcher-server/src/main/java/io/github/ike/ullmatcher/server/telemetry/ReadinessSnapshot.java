package io.github.ike.ullmatcher.server.telemetry;

import java.util.List;

public record ReadinessSnapshot(
        boolean serviceReady,
        boolean clientTrafficReady,
        boolean promotionReady,
        boolean snapshotSyncRequired,
        boolean catchUpInProgress,
        boolean tlsReloadInProgress,
        long transportSecurityGeneration,
        long transportSecurityReloadCount,
        long transportSecurityFailureCount,
        String transportSecurityLastError,
        String syncState,
        List<String> recentErrors,
        LastTickSummary lastTickResult,
        LastGateSummary lastGateDecision,
        String lastTickAction,
        String lastTickReason,
        String lastGateReason,
        long lastGateReceivedLag,
        long lastGateDurableLag,
        long lastGateAppliedLag,
        long lastGateSnapshotLag,
        String transportPolicyStatus,
        String transportPolicyConclusion,
        String transportReconciliationStatus,
        String transportReconciliationConclusion,
        String reason
) {
    public record LastTickSummary(
            String action,
            String roleBefore,
            String roleAfter,
            boolean leaseChanged,
            String reason
    ) {}

    public record LastGateSummary(
            boolean promotionReady,
            boolean catchUpRequired,
            boolean snapshotSyncRequired,
            String reason,
            long receivedLag,
            long durableLag,
            long appliedLag,
            long snapshotLag
    ) {}
}
