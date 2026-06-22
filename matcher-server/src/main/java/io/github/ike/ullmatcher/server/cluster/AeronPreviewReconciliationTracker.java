package io.github.ike.ullmatcher.server.cluster;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

final class AeronPreviewReconciliationTracker {
    private final LongSupplier authoritativeLastReceivedSupplier;
    private final AtomicLong lastPreviewReceivedSequence = new AtomicLong();
    private final AtomicLong previewGapCount = new AtomicLong();
    private final AtomicLong previewOutOfOrderCount = new AtomicLong();
    private final AtomicReference<String> lastAnomaly = new AtomicReference<>("");

    AeronPreviewReconciliationTracker(LongSupplier authoritativeLastReceivedSupplier) {
        this.authoritativeLastReceivedSupplier = authoritativeLastReceivedSupplier;
    }

    synchronized void recordPreviewSequence(long sequence) {
        long previous = lastPreviewReceivedSequence.get();
        if (previous > 0L) {
            long expected = previous + 1L;
            if (sequence > expected) {
                previewGapCount.incrementAndGet();
                lastAnomaly.set("preview gap detected: expected=" + expected + " actual=" + sequence);
            } else if (sequence <= previous) {
                previewOutOfOrderCount.incrementAndGet();
                lastAnomaly.set("preview out-of-order sequence: previous=" + previous + " actual=" + sequence);
            }
        }
        if (sequence > previous) {
            lastPreviewReceivedSequence.set(sequence);
        }
    }

    TransportMetricsSnapshot enrich(TransportMetricsSnapshot base) {
        long authoritativeLastReceived = authoritativeLastReceivedSupplier.getAsLong();
        long previewLastReceived = lastPreviewReceivedSequence.get();
        long gapCount = previewGapCount.get();
        long outOfOrderCount = previewOutOfOrderCount.get();
        String status;
        String conclusion;
        if (previewLastReceived == 0L && authoritativeLastReceived == 0L) {
            status = "IDLE";
            conclusion = "no authoritative or preview replication observed yet";
        } else if (gapCount > 0L || outOfOrderCount > 0L) {
            status = "DIVERGED";
            conclusion = lastAnomaly.get().isBlank() ? "preview sequence anomalies detected" : lastAnomaly.get();
        } else if (previewLastReceived == authoritativeLastReceived) {
            status = "MATCHED";
            conclusion = "preview sequence matches authoritative lastReceivedSequence";
        } else if (previewLastReceived < authoritativeLastReceived) {
            status = "PREVIEW_LAGGING";
            conclusion = "preview lastReceivedSequence lags authoritative gRPC path";
        } else {
            status = "PREVIEW_AHEAD";
            conclusion = "preview lastReceivedSequence is ahead of authoritative gRPC path";
        }
        return base.withReconciliation(
                authoritativeLastReceived,
                previewLastReceived,
                gapCount,
                outOfOrderCount,
                status,
                conclusion
        );
    }
}
