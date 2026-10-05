/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.storage;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.storage.replay.ReplayService;
import io.github.ike.ullmatcher.storage.snapshot.SnapshotStore;
import io.github.ike.ullmatcher.storage.wal.MmapCommandWal;
import io.github.ike.ullmatcher.storage.wal.SegmentedMmapWal;
import io.github.ike.ullmatcher.storage.wal.StorageSync;
import io.github.ike.ullmatcher.storage.wal.WalManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StorageBranchCoverageTest {
    private static final int SYMBOL = 7;

    @Test
    void snapshotRestoreHandlesMissingFileAndRejectsCorruptPayloads(@TempDir Path directory) throws Exception {
        MatcherConfig cfg = new MatcherConfig(SYMBOL, 64, 64, 64, 100L, true);
        Path missing = directory.resolve("none.snap");

        assertEquals(0, SnapshotStore.restore(missing, cfg, new NoopHandler()).snapshotSequence());
        assertTrue(SnapshotStore.scanLiveOrders(missing).isEmpty());

        Path badMagic = directory.resolve("bad-magic.snap");
        Files.write(badMagic, new byte[] {0, 0, 0, 1});
        assertThrows(IOException.class, () -> SnapshotStore.restore(badMagic, cfg, new NoopHandler()));

        Path badVersion = directory.resolve("bad-version.snap");
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(badVersion))) {
            out.writeInt(0x534E4150);
            out.writeInt(99);
        }
        assertThrows(IOException.class, () -> SnapshotStore.restore(badVersion, cfg, new NoopHandler()));
    }

    @Test
    void snapshotRejectsInvalidQuantitySequenceAndTimeInForce(@TempDir Path directory) throws Exception {
        MatcherConfig cfg = new MatcherConfig(SYMBOL, 64, 64, 64, 100L, true);
        Path file = directory.resolve("orders.snap");
        writeSnapshot(file, 1L, 1L, 10L, 5L, 2L, TimeInForce.IOC.code);
        assertThrows(IOException.class, () -> SnapshotStore.restore(file, cfg, new NoopHandler()));

        writeSnapshot(file, 1L, 1L, 10L, 10L, 0L, TimeInForce.GTC.code);
        assertThrows(IOException.class, () -> SnapshotStore.restore(file, cfg, new NoopHandler()));

        writeSnapshot(file, 1L, 1L, 10L, 10L, 9L, TimeInForce.GTC.code);
        assertThrows(IOException.class, () -> SnapshotStore.restore(file, cfg, new NoopHandler()));
    }

    @Test
    void snapshotRestoresPostOnlyAndScanLiveOrders(@TempDir Path directory) throws Exception {
        MatcherConfig cfg = new MatcherConfig(SYMBOL, 64, 64, 64, 100L, true);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(cfg, new NoopHandler());
        matcher.onCommand(Command.newOrder(1, 1, 9, SYMBOL, Side.SELL, OrderType.LIMIT, TimeInForce.POST_ONLY, 100, 2));
        Path file = directory.resolve("post-only.snap");
        SnapshotStore.write(file, matcher);

        SnapshotStore.RestoreResult restored = SnapshotStore.restore(file, cfg, new NoopHandler());
        assertEquals(1, restored.matcher().liveOrderCount());
        assertEquals(TimeInForce.POST_ONLY, SnapshotStore.scanLiveOrders(file).getFirst().timeInForce());
    }

    @Test
    void walManifestRejectsBadVersionSizeAndChecksum(@TempDir Path directory) throws Exception {
        Path segment = directory.resolve("seg.wal");
        Files.writeString(segment, "payload");
        Path manifest = directory.resolve("wal.manifest");
        WalManifest.write(manifest, directory.resolve("snap"), 1L, 0L, List.of(segment));

        Path badVersion = directory.resolve("bad.manifest");
        Files.writeString(badVersion, Files.readString(manifest).replace("version=1", "version=2"));
        assertThrows(IOException.class, () -> WalManifest.read(badVersion));

        Path missingProp = directory.resolve("missing.manifest");
        Files.writeString(missingProp, "version=1\n");
        assertThrows(IOException.class, () -> WalManifest.read(missingProp));

        WalManifest written = WalManifest.read(manifest);
        Files.writeString(segment, "changed-payload");
        IOException checksum = assertThrows(IOException.class, written::validateSegments);
        assertTrue(checksum.getMessage().contains("checksum") || checksum.getMessage().contains("size"));
    }

    @Test
    void mmapWalCoversNonListAppendAllCancelAndOverflow(@TempDir Path directory) throws Exception {
        Path wal = directory.resolve("one.wal");
        try (MmapCommandWal writer = new MmapCommandWal(wal, MmapCommandWal.RECORD_SIZE * 3L)) {
            writer.appendAll(Set.of(
                    Command.cancel(1, 9, SYMBOL),
                    Command.snapshotMarker(2, SYMBOL)
            ));
            writer.append(Command.shutdown(3));
            writer.force();
            assertThrows(IllegalStateException.class, () -> writer.append(Command.shutdown(4)));
        }
        try (MmapCommandWal reader = new MmapCommandWal(wal, MmapCommandWal.RECORD_SIZE * 3L)) {
            assertEquals(3, ReplayService.replay(reader, new UltraLowLatencyMatcher(
                    new MatcherConfig(SYMBOL, 64, 64, 64, 100L, true), new NoopHandler()), 0L));
        }
    }

    @Test
    void segmentedWalRejectsTinySegmentAndEmptyAppendAll(@TempDir Path directory) {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentedMmapWal(directory, "tiny", MmapCommandWal.RECORD_SIZE - 1L));
    }

    @Test
    void storageSyncIgnoresMissingDirectory(@TempDir Path directory) throws Exception {
        StorageSync.forceDirectory(null);
        StorageSync.forceDirectory(directory.resolve("does-not-exist"));
        Path file = directory.resolve("x");
        Files.writeString(file, "ok");
        StorageSync.forceFile(file);
    }

    private static void writeSnapshot(Path file, long snapshotSequence, long tradeId,
                                      long quantity, long remaining, long orderSequence, byte tif) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(file))) {
            out.writeInt(0x534E4150);
            out.writeInt(3);
            out.writeLong(snapshotSequence);
            out.writeLong(tradeId);
            out.writeLong(1L);
            out.writeLong(1L);
            out.writeLong(1L);
            out.writeInt(SYMBOL);
            out.writeByte(Side.SELL.code);
            out.writeByte(tif);
            out.writeLong(100L);
            out.writeLong(quantity);
            out.writeLong(remaining);
            out.writeLong(orderSequence);
            out.writeLong(0L);
        }
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }
}
