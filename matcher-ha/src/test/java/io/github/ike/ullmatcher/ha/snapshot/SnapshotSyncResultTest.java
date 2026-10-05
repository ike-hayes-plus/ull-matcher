package io.github.ike.ullmatcher.ha.snapshot;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SnapshotSyncResultTest {
    private static final Path FILE = Path.of("downloaded-snapshot.bin");

    @Test
    void aZeroByteDownloadIsRepresentable() {
        SnapshotSyncResult result = new SnapshotSyncResult(FILE, 0L, 0L, 0L, 0L);

        assertEquals(FILE, result.file());
        assertEquals(0L, result.bytesWritten());
    }

    @Test
    void downloadMetadataIsExposedAsGiven() {
        SnapshotSyncResult result = new SnapshotSyncResult(FILE, 4_096L, 77L, 12L, 9L);

        assertEquals(4_096L, result.bytesWritten());
        assertEquals(77L, result.lastSequence());
        assertEquals(12L, result.lastTradeId());
        assertEquals(9L, result.liveOrderCount());
        assertTrue(result.toString().contains("bytesWritten=4096"));
    }

    @Test
    void negativeValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new SnapshotSyncResult(FILE, -1L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotSyncResult(FILE, 0L, -1L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotSyncResult(FILE, 0L, 0L, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotSyncResult(FILE, 0L, 0L, 0L, -1L));
        assertThrows(NullPointerException.class, () -> new SnapshotSyncResult(null, 0L, 0L, 0L, 0L));
    }

    @Test
    void resultsDescribingTheSameDownloadAreEqual() {
        SnapshotSyncResult first = new SnapshotSyncResult(FILE, 4_096L, 77L, 12L, 9L);
        SnapshotSyncResult second = new SnapshotSyncResult(FILE, 4_096L, 77L, 12L, 9L);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void aSyncSourceReportsWhereItWroteTheSnapshot() throws IOException {
        SnapshotSyncSource source = new SnapshotSyncSource() {
            @Override
            public String nodeId() {
                return "node-a";
            }

            @Override
            public SnapshotSyncResult downloadLatestSnapshot(Path targetFile, long timeoutNanos) {
                return new SnapshotSyncResult(targetFile, 128L, 5L, 1L, 2L);
            }
        };

        SnapshotSyncResult result = source.downloadLatestSnapshot(FILE, 1_000L);

        assertEquals("node-a", source.nodeId());
        assertEquals(FILE, result.file());
        assertEquals(128L, result.bytesWritten());
    }
}
