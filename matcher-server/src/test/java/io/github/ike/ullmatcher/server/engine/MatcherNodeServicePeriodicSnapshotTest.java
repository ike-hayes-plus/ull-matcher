package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherNodeServicePeriodicSnapshotTest {
    @Test
    void periodicSnapshotCapturesSubmittedSequence() throws Exception {
        Path dir = Files.createTempDirectory("periodic-snapshot");
        MatcherServerConfig config = MatcherServerConfig.builder("node-a", 1, dir)
                .snapshotIntervalMillis(200L)
                .build();
        try (MatcherNodeService service = new MatcherNodeService(config)) {
            service.start();
            service.submitNewOrder(1L, 1L, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 10L, null);
            assertTrue(awaitFile(config.snapshotFile(), 5_000L),
                    "expected periodic snapshot file");
            assertTrue(service.latestSnapshot().lastSequence() >= 1L,
                    "expected periodic snapshot to capture submitted sequence");
        }
    }

    private static boolean awaitFile(Path file, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.exists(file)) {
                return true;
            }
            Thread.sleep(50L);
        }
        return false;
    }
}
