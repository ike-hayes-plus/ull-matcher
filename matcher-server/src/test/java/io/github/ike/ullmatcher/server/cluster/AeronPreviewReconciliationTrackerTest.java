package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class AeronPreviewReconciliationTrackerTest {
    @Test
    void reportsMatchedWhenPreviewAndAuthoritativeSequencesAlign() {
        AtomicLong authoritative = new AtomicLong(7L);
        AeronPreviewReconciliationTracker tracker = new AeronPreviewReconciliationTracker(authoritative::get);
        tracker.recordPreviewSequence(7L);

        TransportMetricsSnapshot snapshot = tracker.enrich(TransportMetricsSnapshot.none("AERON_PREVIEW"));

        assertEquals("MATCHED", snapshot.reconciliationStatus());
        assertEquals(7L, snapshot.authoritativeLastReceivedSequence());
        assertEquals(7L, snapshot.previewLastReceivedSequence());
    }

    @Test
    void reportsDivergedWhenPreviewSequenceHasGap() {
        AtomicLong authoritative = new AtomicLong(5L);
        AeronPreviewReconciliationTracker tracker = new AeronPreviewReconciliationTracker(authoritative::get);
        tracker.recordPreviewSequence(1L);
        tracker.recordPreviewSequence(3L);

        TransportMetricsSnapshot snapshot = tracker.enrich(TransportMetricsSnapshot.none("AERON_PREVIEW"));

        assertEquals("DIVERGED", snapshot.reconciliationStatus());
        assertEquals(1L, snapshot.previewGapCount());
    }
}
