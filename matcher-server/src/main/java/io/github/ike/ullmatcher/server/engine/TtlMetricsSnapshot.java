package io.github.ike.ullmatcher.server.engine;

import java.util.List;

public record TtlMetricsSnapshot(
        boolean enabled,
        long activeTrackedOrders,
        long pendingSubmissions,
        long scheduledTotal,
        long cancelRequestedTotal,
        long cancelAcceptedTotal,
        long cancelSkippedTotal,
        long cancelFailedTotal,
        List<TtlAuditEntry> recentAuditEntries
) {
    public static TtlMetricsSnapshot disabled() {
        return new TtlMetricsSnapshot(false, 0L, 0L, 0L, 0L, 0L, 0L, 0L, List.of());
    }
}
