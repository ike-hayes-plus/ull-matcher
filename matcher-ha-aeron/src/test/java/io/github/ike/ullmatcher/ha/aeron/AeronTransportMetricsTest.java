package io.github.ike.ullmatcher.ha.aeron;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Counter semantics for the Aeron transport metrics. Each counter must be independent so an
 * operator reading the snapshot can attribute a failure to the right transport path.
 */
final class AeronTransportMetricsTest {
    @Test
    void freshMetricsReportZeroForEveryCounter() {
        AeronTransportMetrics.Snapshot snapshot = new AeronTransportMetrics().snapshot();

        assertEquals(0L, snapshot.previewPublishedCommands());
        assertEquals(0L, snapshot.previewPublishedBytes());
        assertEquals(0L, snapshot.previewPublishFailures());
        assertEquals(0L, snapshot.previewReceivedCommands());
        assertEquals(0L, snapshot.previewReceivedBytes());
        assertEquals(0L, snapshot.snapshotRequests());
        assertEquals(0L, snapshot.snapshotRequestFailures());
        assertEquals(0L, snapshot.snapshotBytesSent());
        assertEquals(0L, snapshot.snapshotBytesReceived());
        assertEquals(0L, snapshot.controlRequests());
        assertEquals(0L, snapshot.controlRequestFailures());
    }

    @Test
    void everyRecordMethodUpdatesOnlyItsOwnCounters() {
        AeronTransportMetrics metrics = new AeronTransportMetrics();

        metrics.recordPublished(128);
        metrics.recordPublished(64);
        metrics.recordPublishFailure();
        metrics.recordReceived(32);
        metrics.recordSnapshotRequest();
        metrics.recordSnapshotRequest();
        metrics.recordSnapshotRequestFailure();
        metrics.recordSnapshotBytesSent(1_000);
        metrics.recordSnapshotBytesReceived(2_000);
        metrics.recordControlRequest();
        metrics.recordControlRequestFailure();
        metrics.recordControlRequestFailure();

        AeronTransportMetrics.Snapshot snapshot = metrics.snapshot();

        assertEquals(2L, snapshot.previewPublishedCommands());
        assertEquals(192L, snapshot.previewPublishedBytes());
        assertEquals(1L, snapshot.previewPublishFailures());
        assertEquals(1L, snapshot.previewReceivedCommands());
        assertEquals(32L, snapshot.previewReceivedBytes());
        assertEquals(2L, snapshot.snapshotRequests());
        assertEquals(1L, snapshot.snapshotRequestFailures());
        assertEquals(1_000L, snapshot.snapshotBytesSent());
        assertEquals(2_000L, snapshot.snapshotBytesReceived());
        assertEquals(1L, snapshot.controlRequests());
        assertEquals(2L, snapshot.controlRequestFailures());
    }

    @Test
    void snapshotIsAnImmutableCopyTakenAtCallTime() {
        AeronTransportMetrics metrics = new AeronTransportMetrics();
        metrics.recordReceived(10);

        AeronTransportMetrics.Snapshot first = metrics.snapshot();
        metrics.recordReceived(10);
        AeronTransportMetrics.Snapshot second = metrics.snapshot();

        assertEquals(1L, first.previewReceivedCommands());
        assertEquals(10L, first.previewReceivedBytes());
        assertEquals(2L, second.previewReceivedCommands());
        assertEquals(20L, second.previewReceivedBytes());
    }

    @Test
    void countersAreSafeUnderConcurrentUpdates() throws Exception {
        AeronTransportMetrics metrics = new AeronTransportMetrics();
        int threads = 4;
        int iterations = 2_000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    for (int n = 0; n < iterations; n++) {
                        metrics.recordPublished(1);
                        metrics.recordControlRequest();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(30L, TimeUnit.SECONDS));

        AeronTransportMetrics.Snapshot snapshot = metrics.snapshot();

        assertEquals((long) threads * iterations, snapshot.previewPublishedCommands());
        assertEquals((long) threads * iterations, snapshot.previewPublishedBytes());
        assertEquals((long) threads * iterations, snapshot.controlRequests());
    }
}
