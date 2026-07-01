package io.github.ike.ullmatcher.ring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SpscRingBufferTest {
    @Test
    void rejectsNonPowerOfTwoCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new SpscRingBuffer<>(3));
    }

    @Test
    void offerFailsWhenFullAndRecoversAfterPoll() {
        SpscRingBuffer<String> ring = new SpscRingBuffer<>(2);

        assertTrue(ring.offer("a"));
        assertTrue(ring.offer("b"));
        assertFalse(ring.offer("c"));
        assertEquals(2, ring.size());
        assertEquals(0, ring.remainingCapacity());

        assertEquals("a", ring.poll());
        assertTrue(ring.offer("c"));
        assertEquals("b", ring.poll());
        assertEquals("c", ring.poll());
        assertNull(ring.poll());
    }

    @Test
    void drainHonorsLimitAndPreservesOrder() {
        SpscRingBuffer<String> ring = new SpscRingBuffer<>(4);
        assertTrue(ring.offer("a"));
        assertTrue(ring.offer("b"));
        assertTrue(ring.offer("c"));

        String[] batch = new String[2];
        assertEquals(2, ring.drain(batch, batch.length));
        assertArrayEquals(new String[]{"a", "b"}, batch);
        assertEquals("c", ring.poll());
    }
}
