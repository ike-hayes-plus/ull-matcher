package io.github.ike.ullmatcher.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LongHeapTest {
    @Test
    void maxHeapPollsLargestFirst() {
        LongHeap heap = new LongHeap(2, true);

        heap.add(10L);
        heap.add(30L);
        heap.add(20L);

        assertEquals(30L, heap.poll());
        assertEquals(20L, heap.poll());
        assertEquals(10L, heap.poll());
        assertTrue(heap.isEmpty());
    }

    @Test
    void minHeapPollsSmallestFirst() {
        LongHeap heap = new LongHeap(2, false);

        heap.add(10L);
        heap.add(30L);
        heap.add(20L);

        assertEquals(10L, heap.poll());
        assertEquals(20L, heap.poll());
        assertEquals(30L, heap.poll());
    }

    @Test
    void copyIntoPreservesSourceAndRequiresSameDirection() {
        LongHeap source = new LongHeap(2, true);
        source.add(10L);
        source.add(30L);
        LongHeap target = new LongHeap(2, true);

        source.copyInto(target);

        assertEquals(30L, target.poll());
        assertEquals(30L, source.poll());
        assertThrows(IllegalArgumentException.class, () -> source.copyInto(new LongHeap(2, false)));
    }
}
