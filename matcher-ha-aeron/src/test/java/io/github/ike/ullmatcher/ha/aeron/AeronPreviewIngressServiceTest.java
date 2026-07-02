package io.github.ike.ullmatcher.ha.aeron;

import io.aeron.Aeron;
import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class AeronPreviewIngressServiceTest {
    @Test
    void ingressRecordsPreviewTraffic() throws Exception {
        String channel = "aeron:udp?endpoint=127.0.0.1:16001";
        int streamId = 11001;
        Path directory = Files.createTempDirectory("aeron-preview");
        AeronTransportMetrics metrics = new AeronTransportMetrics();
        try (AeronPreviewIngressService ingressService = new AeronPreviewIngressService(channel, streamId, directory, metrics)) {
            Thread.sleep(200L);
            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory.toAbsolutePath().toString()));
                 AeronShadowReplicationTarget target = new AeronShadowReplicationTarget(aeron, channel, streamId, metrics)) {
                assertTrue(!ingressService.aeronDirectoryName().isBlank());
                target.publish(Command.newOrder(1L, 11L, 7L, 1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L),
                        TimeUnit.SECONDS.toNanos(2));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (System.nanoTime() < deadline && metrics.snapshot().previewReceivedCommands() == 0L) {
                    Thread.sleep(10L);
                }
                assertTrue(metrics.snapshot().previewReceivedCommands() > 0L);
            }
        }
    }
}
