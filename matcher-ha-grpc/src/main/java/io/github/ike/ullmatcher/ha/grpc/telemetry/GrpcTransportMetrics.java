package io.github.ike.ullmatcher.ha.grpc.telemetry;

import java.util.concurrent.atomic.LongAdder;

public final class GrpcTransportMetrics {
    private final LongAdder unaryReplications = new LongAdder();
    private final LongAdder streamedBatches = new LongAdder();
    private final LongAdder streamedCommands = new LongAdder();
    private final LongAdder snapshotBytesSent = new LongAdder();
    private final LongAdder snapshotBytesReceived = new LongAdder();
    private final LongAdder rejectedIngress = new LongAdder();
    private final LongAdder failures = new LongAdder();

    public void recordUnaryReplication() {
        unaryReplications.increment();
    }

    public void recordStreamBatch(int ackedCommands) {
        streamedBatches.increment();
        streamedCommands.add(ackedCommands);
    }

    public void recordSnapshotBytesSent(long bytes) {
        snapshotBytesSent.add(bytes);
    }

    public void recordSnapshotBytesReceived(long bytes) {
        snapshotBytesReceived.add(bytes);
    }

    public void recordRejectedIngress() {
        rejectedIngress.increment();
    }

    public void recordFailure() {
        failures.increment();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                unaryReplications.sum(),
                streamedBatches.sum(),
                streamedCommands.sum(),
                snapshotBytesSent.sum(),
                snapshotBytesReceived.sum(),
                rejectedIngress.sum(),
                failures.sum()
        );
    }

    public record Snapshot(
            long unaryReplications,
            long streamedBatches,
            long streamedCommands,
            long snapshotBytesSent,
            long snapshotBytesReceived,
            long rejectedIngress,
            long failures
    ) {}
}
