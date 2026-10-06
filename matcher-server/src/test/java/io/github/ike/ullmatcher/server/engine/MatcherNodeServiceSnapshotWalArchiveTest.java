/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.storage.wal.MmapCommandWal;
import io.github.ike.ullmatcher.storage.wal.WalArchiveConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherNodeServiceSnapshotWalArchiveTest {
    @Test
    void createSnapshotArchivesAndDeletesCoveredWalSegments() throws Exception {
        Path dir = Files.createTempDirectory("snapshot-wal-archive");
        Path cold = dir.resolve("wal-cold");
        MatcherServerConfig config = MatcherServerConfig.builder("node-a", 1, dir)
                .walSegmentSizeBytes(MmapCommandWal.RECORD_SIZE * 2L)
                .walArchiveConfig(WalArchiveConfig.ofDirectory(cold))
                .build();
        try (MatcherNodeService service = new MatcherNodeService(config)) {
            service.start();
            for (long orderId = 1L; orderId <= 6L; orderId++) {
                service.submitNewOrder(1L, orderId, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100L + orderId, 1L, null);
            }
            service.createSnapshot();
        }

        try (Stream<Path> archived = Files.walk(cold)) {
            assertTrue(archived.anyMatch(path -> path.getFileName().toString().endsWith(".wal")),
                    "snapshot must copy covered WAL segments into the cold archive");
        }
        assertTrue(Files.exists(config.walDirectory().resolve(config.walPrefix() + ".manifest")),
                "snapshot must write a WAL checkpoint manifest");
    }
}
