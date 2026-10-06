package io.github.ike.ullmatcher.server.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FencedRejoinStoreTest {
    @Test
    void completeIfNeededInstallsDownloadedSnapshotAndQuarantinesWal(@TempDir Path dir) throws Exception {
        Path snapshot = dir.resolve("symbol-1.snap");
        Path wal = dir.resolve("wal");
        Path rejoin = FencedRejoinStore.rejoinTemp(snapshot);
        Path quarantine = dir.resolve("wal.fenced.test");
        Files.createDirectories(wal);
        Files.writeString(wal.resolve("tail.wal"), "local-tail");
        Files.writeString(rejoin, "authoritative");
        FencedRejoinStore.writeIntent(snapshot, rejoin, quarantine);

        FencedRejoinStore.completeIfNeeded(snapshot, wal);

        assertTrue(Files.exists(snapshot));
        assertTrue(Files.readString(snapshot).contains("authoritative"));
        assertFalse(Files.exists(wal));
        assertTrue(Files.exists(quarantine.resolve("tail.wal")));
        assertFalse(Files.exists(FencedRejoinStore.intentFile(snapshot)));
    }

    @Test
    void completeIfNeededIsNoOpWithoutIntent(@TempDir Path dir) throws Exception {
        Path snapshot = dir.resolve("symbol-1.snap");
        Path wal = dir.resolve("wal");
        Files.createDirectories(wal);
        Files.writeString(wal.resolve("tail.wal"), "keep");

        FencedRejoinStore.completeIfNeeded(snapshot, wal);

        assertTrue(Files.exists(wal.resolve("tail.wal")));
        assertFalse(Files.exists(snapshot));
    }
}
