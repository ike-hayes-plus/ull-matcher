package io.github.ike.ullmatcher.ha.transport;

/**
 * 汇总单节点复制传输的运行指标、对账状态与安全重载状态。
 */
public record TransportMetricsSnapshot(
        String transportType,
        long previewPublishedCommands,
        long previewPublishedBytes,
        long previewPublishFailures,
        long previewReceivedCommands,
        long previewReceivedBytes,
        long snapshotRequests,
        long snapshotRequestFailures,
        long snapshotBytesSent,
        long snapshotBytesReceived,
        long controlRequests,
        long controlRequestFailures,
        long authoritativeLastReceivedSequence,
        long previewLastReceivedSequence,
        long previewGapCount,
        long previewOutOfOrderCount,
        String reconciliationStatus,
        String reconciliationConclusion,
        String policyStatus,
        String policyConclusion
) {
    public static TransportMetricsSnapshot none(String transportType) {
        return new TransportMetricsSnapshot(
                transportType,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "DISABLED",
                "sequence reconciliation is disabled for this transport mode",
                "STABLE",
                "transport policy is stable"
        );
    }

    public TransportMetricsSnapshot withPolicy(String status, String conclusion) {
        return new TransportMetricsSnapshot(
                transportType,
                previewPublishedCommands,
                previewPublishedBytes,
                previewPublishFailures,
                previewReceivedCommands,
                previewReceivedBytes,
                snapshotRequests,
                snapshotRequestFailures,
                snapshotBytesSent,
                snapshotBytesReceived,
                controlRequests,
                controlRequestFailures,
                authoritativeLastReceivedSequence,
                previewLastReceivedSequence,
                previewGapCount,
                previewOutOfOrderCount,
                reconciliationStatus,
                reconciliationConclusion,
                status,
                conclusion
        );
    }

    public TransportMetricsSnapshot withReconciliation(long authoritativeSequence,
                                                       long previewSequence,
                                                       long gapCount,
                                                       long outOfOrderCount,
                                                       String status,
                                                       String conclusion) {
        return new TransportMetricsSnapshot(
                transportType,
                previewPublishedCommands,
                previewPublishedBytes,
                previewPublishFailures,
                previewReceivedCommands,
                previewReceivedBytes,
                snapshotRequests,
                snapshotRequestFailures,
                snapshotBytesSent,
                snapshotBytesReceived,
                controlRequests,
                controlRequestFailures,
                authoritativeSequence,
                previewSequence,
                gapCount,
                outOfOrderCount,
                status,
                conclusion,
                policyStatus,
                policyConclusion
        );
    }
}
