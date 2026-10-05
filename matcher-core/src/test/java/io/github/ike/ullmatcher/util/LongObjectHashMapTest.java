package io.github.ike.ullmatcher.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LongObjectHashMapTest {
    @Test
    void putGetAndRemoveRoundTrip() {
        LongObjectHashMap<String> map = new LongObjectHashMap<>(8);

        assertNull(map.put(1L, "a"));
        assertNull(map.put(2L, "b"));
        assertEquals("a", map.get(1L));
        assertEquals("b", map.get(2L));
        assertEquals(2, map.size());

        assertEquals("a", map.put(1L, "a2"));
        assertEquals("a2", map.get(1L));
        assertEquals(2, map.size());

        assertEquals("a2", map.remove(1L));
        assertNull(map.get(1L));
        assertNull(map.remove(1L));
        assertEquals(1, map.size());
    }

    @Test
    void zeroKeyIsReservedOnEveryEntryPoint() {
        LongObjectHashMap<String> map = new LongObjectHashMap<>(4);

        assertThrows(IllegalArgumentException.class, () -> map.get(0L));
        assertThrows(IllegalArgumentException.class, () -> map.put(0L, "x"));
        assertThrows(IllegalArgumentException.class, () -> map.remove(0L));
        assertThrows(IllegalArgumentException.class, () -> map.canInsertWithoutResize(0L));
    }

    @Test
    void growsBeyondTheInitialCapacityWithoutLosingEntries() {
        LongObjectHashMap<Long> map = new LongObjectHashMap<>(4);

        for (long key = 1L; key <= 500L; key++) {
            map.put(key, key * 10L);
        }

        assertEquals(500, map.size());
        for (long key = 1L; key <= 500L; key++) {
            assertEquals(key * 10L, map.get(key), "key " + key);
        }
    }

    @Test
    void removalKeepsLaterProbeChainEntriesReachable() {
        LongObjectHashMap<Long> map = new LongObjectHashMap<>(1024);
        // Dense sequential keys guarantee collisions and long probe clusters.
        for (long key = 1L; key <= 300L; key++) {
            map.put(key, key);
        }

        for (long key = 1L; key <= 300L; key += 3L) {
            assertEquals(key, map.remove(key));
        }
        for (long key = 1L; key <= 300L; key++) {
            if (key % 3L == 1L) {
                assertNull(map.get(key), "removed key " + key);
            } else {
                assertEquals(key, map.get(key), "surviving key " + key);
            }
        }
    }

    @Test
    void containsKeyTracksInsertionsAndRemovals() {
        LongObjectHashMap<String> map = new LongObjectHashMap<>(4);
        map.put(7L, "v");

        assertTrue(map.containsKey(7L));
        assertFalse(map.containsKey(8L));

        map.remove(7L);
        assertFalse(map.containsKey(7L));
    }

    @Test
    void canInsertWithoutResizeReportsRemainingHeadroom() {
        LongObjectHashMap<String> map = new LongObjectHashMap<>(4);

        assertTrue(map.canInsertWithoutResize(1L));
        for (long key = 1L; key <= 4L; key++) {
            map.put(key, "v" + key);
        }

        assertTrue(map.canInsertWithoutResize(1L), "existing keys never need headroom");
        assertFalse(map.canInsertWithoutResize(999L));
    }

    @Test
    void forEachValueVisitsEveryLiveEntryOnce() {
        LongObjectHashMap<String> map = new LongObjectHashMap<>(8);
        map.put(1L, "a");
        map.put(2L, "b");
        map.put(3L, "c");
        map.remove(2L);

        List<String> visited = new ArrayList<>();
        map.forEachValue(visited::add);

        assertEquals(2, visited.size());
        assertTrue(visited.contains("a"));
        assertTrue(visited.contains("c"));
    }

    @Test
    void handlesNegativeAndExtremeKeys() {
        LongObjectHashMap<String> map = new LongObjectHashMap<>(4);

        map.put(Long.MIN_VALUE, "min");
        map.put(Long.MAX_VALUE, "max");
        map.put(-1L, "neg");

        assertEquals("min", map.get(Long.MIN_VALUE));
        assertEquals("max", map.get(Long.MAX_VALUE));
        assertEquals("neg", map.get(-1L));
    }
}
