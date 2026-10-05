package io.github.ike.ullmatcher.ha.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ReplicationCursorTest {
    @Test
    void promotionWatermarkIsTheMinimumOfDurableAndApplied() {
        assertEquals(7L, new ReplicationCursor(20L, 7L, 9L, 0L).promotionWatermark());
        assertEquals(7L, new ReplicationCursor(20L, 9L, 7L, 0L).promotionWatermark());
        assertEquals(9L, new ReplicationCursor(20L, 9L, 9L, 0L).promotionWatermark());
    }

    @Test
    void promotionWatermarkIgnoresDataThatIsOnlyReceived() {
        ReplicationCursor receivedButNotDurable = new ReplicationCursor(1_000L, 0L, 0L, 0L);

        assertEquals(0L, receivedButNotDurable.promotionWatermark(),
                "received-but-not-durable commands must not count towards promotion");
    }

    @Test
    void anyNegativeSequenceIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicationCursor(-1L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new ReplicationCursor(0L, -1L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new ReplicationCursor(0L, 0L, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new ReplicationCursor(0L, 0L, 0L, -1L));
    }

    @Test
    void theZeroCursorIsValid() {
        ReplicationCursor zero = new ReplicationCursor(0L, 0L, 0L, 0L);

        assertEquals(0L, zero.promotionWatermark());
        assertEquals(new ReplicationCursor(0L, 0L, 0L, 0L), zero);
    }
}
