package io.github.ike.ullmatcher.queue;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MpscArrayQueueTest {
    @Test
    void rejectsNonPowerOfTwoCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new MpscArrayQueue<>(6));
    }

    @Test
    void offerFailsWhenFullAndRecoversAfterPoll() {
        MpscArrayQueue<String> queue = new MpscArrayQueue<>(2);

        assertTrue(queue.offer("a"));
        assertTrue(queue.offer("b"));
        assertFalse(queue.offer("c"));
        assertEquals(2, queue.size());

        assertEquals("a", queue.poll());
        assertTrue(queue.offer("c"));
        assertEquals("b", queue.poll());
        assertEquals("c", queue.poll());
        assertNull(queue.poll());
    }

    @Test
    void drainToHonorsLimitAndPreservesOrder() {
        MpscArrayQueue<String> queue = new MpscArrayQueue<>(4);
        assertTrue(queue.offer("a"));
        assertTrue(queue.offer("b"));
        assertTrue(queue.offer("c"));

        List<String> drained = new ArrayList<>();
        assertEquals(2, queue.drainTo(drained, 2));
        assertEquals(List.of("a", "b"), drained);
        assertEquals("c", queue.poll());
    }
}
