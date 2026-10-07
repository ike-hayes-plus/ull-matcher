package io.github.ike.ullmatcher.ha.transport;

/**
 * 汇总单节点复制传输的运行指标、对账状态与安全重载状态。
 */
public record TransportMetricsSnapshot(
        String transportType,
        long publishedCommands,
        long publishedBytes,
        long publishFailures,
        long receivedCommands,
        long receivedBytes,
        long snapshotRequests,
        long snapshotRequestFailures,
        long snapshotBytesSent,
        long snapshotBytesReceived,
        long controlRequests,
        long controlRequestFailures,
        long authoritativeLastReceivedSequence,
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
                "DISABLED",
                "sequence reconciliation is disabled for this transport mode",
                "STABLE",
                "transport policy is stable"
        );
    }

    public TransportMetricsSnapshot withPolicy(String status, String conclusion) {
        return new TransportMetricsSnapshot(
                transportType,
                publishedCommands,
                publishedBytes,
                publishFailures,
                receivedCommands,
                receivedBytes,
                snapshotRequests,
                snapshotRequestFailures,
                snapshotBytesSent,
                snapshotBytesReceived,
                controlRequests,
                controlRequestFailures,
                authoritativeLastReceivedSequence,
                reconciliationStatus,
                reconciliationConclusion,
                status,
                conclusion
        );
    }

    public TransportMetricsSnapshot withReconciliation(long authoritativeSequence,
                                                       String status,
                                                       String conclusion) {
        return new TransportMetricsSnapshot(
                transportType,
                publishedCommands,
                publishedBytes,
                publishFailures,
                receivedCommands,
                receivedBytes,
                snapshotRequests,
                snapshotRequestFailures,
                snapshotBytesSent,
                snapshotBytesReceived,
                controlRequests,
                controlRequestFailures,
                authoritativeSequence,
                status,
                conclusion,
                policyStatus,
                policyConclusion
        );
    }
}
