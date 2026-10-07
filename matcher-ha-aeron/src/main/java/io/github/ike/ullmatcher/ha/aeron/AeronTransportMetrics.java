package io.github.ike.ullmatcher.ha.aeron;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 汇总 Aeron 复制、快照与控制面传输指标。
 */
public final class AeronTransportMetrics {
    private final AtomicLong publishedCommands = new AtomicLong();
    private final AtomicLong publishedBytes = new AtomicLong();
    private final AtomicLong publishFailures = new AtomicLong();
    private final AtomicLong receivedCommands = new AtomicLong();
    private final AtomicLong receivedBytes = new AtomicLong();
    private final AtomicLong snapshotRequests = new AtomicLong();
    private final AtomicLong snapshotRequestFailures = new AtomicLong();
    private final AtomicLong snapshotBytesSent = new AtomicLong();
    private final AtomicLong snapshotBytesReceived = new AtomicLong();
    private final AtomicLong controlRequests = new AtomicLong();
    private final AtomicLong controlRequestFailures = new AtomicLong();

    public void recordPublished(int bytes) {
        publishedCommands.incrementAndGet();
        publishedBytes.addAndGet(bytes);
    }

    public void recordPublishFailure() {
        publishFailures.incrementAndGet();
    }

    public void recordReceived(int bytes) {
        receivedCommands.incrementAndGet();
        receivedBytes.addAndGet(bytes);
    }

    public Snapshot snapshot() {
        return new Snapshot(
                publishedCommands.get(),
                publishedBytes.get(),
                publishFailures.get(),
                receivedCommands.get(),
                receivedBytes.get(),
                snapshotRequests.get(),
                snapshotRequestFailures.get(),
                snapshotBytesSent.get(),
                snapshotBytesReceived.get(),
                controlRequests.get(),
                controlRequestFailures.get()
        );
    }

    public void recordSnapshotRequest() {
        snapshotRequests.incrementAndGet();
    }

    public void recordSnapshotRequestFailure() {
        snapshotRequestFailures.incrementAndGet();
    }

    public void recordSnapshotBytesSent(int bytes) {
        snapshotBytesSent.addAndGet(bytes);
    }

    public void recordSnapshotBytesReceived(int bytes) {
        snapshotBytesReceived.addAndGet(bytes);
    }

    public void recordControlRequest() {
        controlRequests.incrementAndGet();
    }

    public void recordControlRequestFailure() {
        controlRequestFailures.incrementAndGet();
    }

    public record Snapshot(
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
            long controlRequestFailures
    ) {}
}
