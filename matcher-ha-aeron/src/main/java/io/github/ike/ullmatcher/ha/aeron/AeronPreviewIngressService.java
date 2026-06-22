package io.github.ike.ullmatcher.ha.aeron;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * 接收影子复制通道上的命令并记录预览传输指标。
 */
public final class AeronPreviewIngressService implements Closeable {
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription subscription;
    private final AeronTransportMetrics metrics;
    private final Thread pollerThread;
    private volatile boolean running = true;

    public AeronPreviewIngressService(String channel,
                                      int streamId,
                                      Path aeronDirectory,
                                      AeronTransportMetrics metrics) {
        this(channel, streamId, aeronDirectory, metrics, ignored -> {});
    }

    public AeronPreviewIngressService(String channel,
                                      int streamId,
                                      Path aeronDirectory,
                                      AeronTransportMetrics metrics,
                                      LongConsumer sequenceObserver) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(aeronDirectory, "aeronDirectory");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(sequenceObserver, "sequenceObserver");
        MediaDriver.Context driverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectory.toAbsolutePath().toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        this.mediaDriver = MediaDriver.launchEmbedded(driverContext);
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
        this.subscription = aeron.addSubscription(channel, streamId);
        FragmentHandler handler = (buffer, offset, length, header) -> {
            var command = AeronCommandCodec.decode(buffer, offset, length, header);
            metrics.recordReceived(length);
            sequenceObserver.accept(command.sequence);
        };
        this.pollerThread = Thread.ofPlatform().name("aeron-preview-ingress-" + streamId).start(() -> pollLoop(handler));
    }

    private void pollLoop(FragmentHandler handler) {
        while (running) {
            int fragments = subscription.poll(handler, 32);
            if (fragments == 0) {
                Thread.onSpinWait();
            }
        }
    }

    public String aeronDirectoryName() {
        return mediaDriver.aeronDirectoryName();
    }

    @Override
    public void close() throws IOException {
        running = false;
        try {
            pollerThread.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while closing Aeron preview ingress", e);
        } finally {
            CloseHelper.quietClose(subscription);
            CloseHelper.quietClose(aeron);
            CloseHelper.quietClose(mediaDriver);
        }
    }
}
