package io.github.ike.ullmatcher.ha.snapshot;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SnapshotMaterialTest {
    private static final Path FILE = Path.of("snapshot-000042.bin");

    @Test
    void anEmptySnapshotAtSequenceZeroIsValid() {
        SnapshotMaterial material = new SnapshotMaterial(FILE, 0L, 0L, 0L);

        assertEquals(FILE, material.file());
        assertEquals(0L, material.lastSequence());
        assertEquals(0L, material.lastTradeId());
        assertEquals(0L, material.liveOrderCount());
    }

    @Test
    void metadataIsExposedAsGiven() {
        SnapshotMaterial material = new SnapshotMaterial(FILE, 42L, 17L, 3L);

        assertEquals(42L, material.lastSequence());
        assertEquals(17L, material.lastTradeId());
        assertEquals(3L, material.liveOrderCount());
        assertTrue(material.toString().contains("lastSequence=42"));
    }

    @Test
    void negativeMetadataIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new SnapshotMaterial(FILE, -1L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotMaterial(FILE, 0L, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotMaterial(FILE, 0L, 0L, -1L));
        assertThrows(NullPointerException.class, () -> new SnapshotMaterial(null, 0L, 0L, 0L));
    }

    @Test
    void materialsDescribingTheSameFileAreEqual() {
        SnapshotMaterial first = new SnapshotMaterial(FILE, 42L, 17L, 3L);
        SnapshotMaterial second = new SnapshotMaterial(FILE, 42L, 17L, 3L);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void aSourceCanHandOutTheMaterialItHolds() throws IOException {
        SnapshotMaterial material = new SnapshotMaterial(FILE, 42L, 17L, 3L);
        SnapshotMaterialSource source = () -> material;

        assertSame(material, source.latestSnapshot());
    }
}
