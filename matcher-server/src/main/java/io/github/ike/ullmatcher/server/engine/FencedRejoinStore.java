package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.storage.wal.StorageSync;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * Durable intent for authoritative snapshot install so a crash between WAL
 * quarantine and snapshot install can finish or roll forward on the next start.
 */
final class FencedRejoinStore {
    private static final String REJOIN_KEY = "rejoin";
    private static final String QUARANTINE_KEY = "quarantine";

    private FencedRejoinStore() {}

    static Path intentFile(Path snapshotFile) {
        return snapshotFile.resolveSibling(snapshotFile.getFileName() + ".rejoin.intent");
    }

    static Path rejoinTemp(Path snapshotFile) {
        return snapshotFile.resolveSibling(snapshotFile.getFileName() + ".rejoin");
    }

    static Path plannedQuarantine(Path walDirectory) {
        return walDirectory.resolveSibling(walDirectory.getFileName() + ".fenced."
                + System.currentTimeMillis() + "." + System.nanoTime());
    }

    static void writeIntent(Path snapshotFile, Path rejoinTemp, Path quarantineDir) throws IOException {
        Path intent = intentFile(snapshotFile);
        Properties properties = new Properties();
        properties.setProperty(REJOIN_KEY, rejoinTemp.toAbsolutePath().toString());
        properties.setProperty(QUARANTINE_KEY, quarantineDir.toAbsolutePath().toString());
        Path tmp = intent.resolveSibling(intent.getFileName() + ".tmp");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            properties.store(out, "fenced rejoin");
        }
        StorageSync.forceFile(tmp);
        Files.move(tmp, intent, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        StorageSync.forceDirectory(intent.toAbsolutePath().getParent());
    }

    static void clearIntent(Path snapshotFile) throws IOException {
        Path intent = intentFile(snapshotFile);
        Files.deleteIfExists(intent);
        StorageSync.forceDirectory(intent.toAbsolutePath().getParent());
    }

    static void completeIfNeeded(Path snapshotFile, Path walDirectory) throws IOException {
        Path intent = intentFile(snapshotFile);
        if (!Files.exists(intent)) {
            return;
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(intent)) {
            properties.load(in);
        }
        Path rejoinTemp = Path.of(required(properties, REJOIN_KEY));
        Path quarantineDir = Path.of(required(properties, QUARANTINE_KEY));
        if (Files.exists(rejoinTemp)) {
            StorageSync.forceFile(rejoinTemp);
            Files.move(rejoinTemp, snapshotFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            StorageSync.forceDirectory(snapshotFile.toAbsolutePath().getParent());
        }
        if (Files.exists(walDirectory) && !Files.exists(quarantineDir)) {
            Files.move(walDirectory, quarantineDir);
            StorageSync.forceDirectory(walDirectory.toAbsolutePath().getParent());
        }
        clearIntent(snapshotFile);
    }

    private static String required(Properties properties, String key) throws IOException {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IOException("fenced rejoin intent missing " + key);
        }
        return value;
    }
}
