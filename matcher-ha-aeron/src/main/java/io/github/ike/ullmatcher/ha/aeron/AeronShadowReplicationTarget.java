package io.github.ike.ullmatcher.ha.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.github.ike.ullmatcher.api.Command;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 将复制命令发布到 Aeron 影子通道，用于对账和传输验证。
 */
public final class AeronShadowReplicationTarget implements Closeable {
    private final Aeron aeron;
    private final Publication publication;
    private final UnsafeBuffer buffer = AeronCommandCodec.allocateBuffer();
    private final AeronTransportMetrics metrics;

    public AeronShadowReplicationTarget(Aeron aeron, String channel, int streamId, AeronTransportMetrics metrics) {
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.publication = aeron.addExclusivePublication(Objects.requireNonNull(channel, "channel"), streamId);
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    public void publish(Command command, long timeoutNanos) throws IOException {
        Objects.requireNonNull(command, "command");
        int length = AeronCommandCodec.encode(command, buffer);
        long deadline = timeoutNanos <= 0L ? Long.MAX_VALUE : System.nanoTime() + timeoutNanos;
        for (;;) {
            long result = publication.offer(buffer, 0, length);
            if (result > 0L) {
                metrics.recordPublished(length);
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                metrics.recordPublishFailure();
                throw new IOException("Aeron preview publication unavailable: result=" + result);
            }
            if (System.nanoTime() >= deadline) {
                metrics.recordPublishFailure();
                throw new IOException("Aeron preview publication timed out");
            }
            Thread.onSpinWait();
        }
    }

    @Override
    public void close() {
        publication.close();
        aeron.close();
    }
}
