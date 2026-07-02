package io.github.ike.ullmatcher.ha.aeron;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 汇总 Aeron 复制、快照与控制面传输指标。
 */
public final class AeronTransportMetrics {
    private final AtomicLong previewPublishedCommands = new AtomicLong();
    private final AtomicLong previewPublishedBytes = new AtomicLong();
    private final AtomicLong previewPublishFailures = new AtomicLong();
    private final AtomicLong previewReceivedCommands = new AtomicLong();
    private final AtomicLong previewReceivedBytes = new AtomicLong();
    private final AtomicLong snapshotRequests = new AtomicLong();
    private final AtomicLong snapshotRequestFailures = new AtomicLong();
    private final AtomicLong snapshotBytesSent = new AtomicLong();
    private final AtomicLong snapshotBytesReceived = new AtomicLong();
    private final AtomicLong controlRequests = new AtomicLong();
    private final AtomicLong controlRequestFailures = new AtomicLong();

    public void recordPublished(int bytes) {
        previewPublishedCommands.incrementAndGet();
        previewPublishedBytes.addAndGet(bytes);
    }

    public void recordPublishFailure() {
        previewPublishFailures.incrementAndGet();
    }

    public void recordReceived(int bytes) {
        previewReceivedCommands.incrementAndGet();
        previewReceivedBytes.addAndGet(bytes);
    }

    public Snapshot snapshot() {
        return new Snapshot(
                previewPublishedCommands.get(),
                previewPublishedBytes.get(),
                previewPublishFailures.get(),
                previewReceivedCommands.get(),
                previewReceivedBytes.get(),
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
            long controlRequestFailures
    ) {}
}
