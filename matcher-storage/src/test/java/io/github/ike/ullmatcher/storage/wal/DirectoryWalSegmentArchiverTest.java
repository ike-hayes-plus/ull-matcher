package io.github.ike.ullmatcher.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectoryWalSegmentArchiverTest {
    @TempDir
    Path tempDir;

    @Test
    void copiesSegmentIntoShardAndNodeLayout() throws Exception {
        Path hotWal = tempDir.resolve("hot-wal");
        Files.createDirectories(hotWal);
        Path segment = hotWal.resolve("symbol-1.000001.wal");
        byte[] payload = new byte[] {1, 2, 3, 4};
        Files.write(segment, payload);

        Path coldRoot = tempDir.resolve("cold");
        DirectoryWalSegmentArchiver archiver = new DirectoryWalSegmentArchiver(coldRoot, "merchant:42", "node-a");
        archiver.archive(segment);

        Path archived = archiver.destinationDirectory().resolve(segment.getFileName());
        assertTrue(Files.isRegularFile(archived));
        assertArrayEquals(payload, Files.readAllBytes(archived));
        assertTrue(Files.exists(segment), "archiver must not delete hot segment; SegmentedMmapWal does that");
    }

    @Test
    void walArchiveConfigBuildsArchiverOrNoop() throws Exception {
        assertFalse(WalArchiveConfig.disabled().enabled());
        WalSegmentArchiver noop = WalArchiveConfig.disabled().archiver("node-a", "symbol-1");
        noop.archive(tempDir.resolve("unused.wal"));

        Path cold = tempDir.resolve("archive");
        WalArchiveConfig enabled = WalArchiveConfig.ofDirectory(cold);
        assertTrue(enabled.enabled());
        assertTrue(WalArchiveConfig.fromProperty("").enabled() == false);
        assertTrue(WalArchiveConfig.fromProperty("  ").enabled() == false);
        assertEqualsPath(cold, WalArchiveConfig.fromProperty(cold.toString()).coldArchiveDirectory());
    }

    private static void assertEqualsPath(Path expected, Path actual) {
        org.junit.jupiter.api.Assertions.assertEquals(expected.toAbsolutePath().normalize(), actual);
    }
}
