package io.github.ike.ullmatcher.ha.grpc.telemetry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class GrpcTransportMetricsTest {
    @Test
    void freshMetricsSnapshotIsAllZero() {
        GrpcTransportMetrics.Snapshot snapshot = new GrpcTransportMetrics().snapshot();

        assertEquals(0L, snapshot.unaryReplications());
        assertEquals(0L, snapshot.streamedBatches());
        assertEquals(0L, snapshot.streamedCommands());
        assertEquals(0L, snapshot.snapshotBytesSent());
        assertEquals(0L, snapshot.snapshotBytesReceived());
        assertEquals(0L, snapshot.rejectedIngress());
        assertEquals(0L, snapshot.failures());
    }

    @Test
    void everyCounterIsRecordedIndependently() {
        GrpcTransportMetrics metrics = new GrpcTransportMetrics();

        metrics.recordUnaryReplication();
        metrics.recordUnaryReplication();
        metrics.recordStreamBatch(12);
        metrics.recordStreamBatch(30);
        metrics.recordSnapshotBytesSent(4_096L);
        metrics.recordSnapshotBytesReceived(2_048L);
        metrics.recordRejectedIngress();
        metrics.recordFailure();

        GrpcTransportMetrics.Snapshot snapshot = metrics.snapshot();

        assertEquals(2L, snapshot.unaryReplications());
        assertEquals(2L, snapshot.streamedBatches());
        assertEquals(42L, snapshot.streamedCommands(), "streamed commands accumulate across batches");
        assertEquals(4_096L, snapshot.snapshotBytesSent());
        assertEquals(2_048L, snapshot.snapshotBytesReceived());
        assertEquals(1L, snapshot.rejectedIngress());
        assertEquals(1L, snapshot.failures());
    }

    @Test
    void snapshotIsAPointInTimeCopy() {
        GrpcTransportMetrics metrics = new GrpcTransportMetrics();
        metrics.recordUnaryReplication();

        GrpcTransportMetrics.Snapshot before = metrics.snapshot();
        metrics.recordUnaryReplication();

        assertEquals(1L, before.unaryReplications());
        assertEquals(2L, metrics.snapshot().unaryReplications());
    }
}
