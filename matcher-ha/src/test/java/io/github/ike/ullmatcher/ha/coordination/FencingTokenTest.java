package io.github.ike.ullmatcher.ha.coordination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fencing tokens are the anti-split-brain primitive: epoch zero must be impossible so that "no
 * epoch" can never be mistaken for a valid one, and {@code next()} must strictly increase.
 */
final class FencingTokenTest {
    @Test
    void nextStrictlyIncreasesTheEpochWithoutMutatingTheOriginal() {
        FencingToken token = new FencingToken(1L);

        FencingToken next = token.next();

        assertEquals(2L, next.epoch());
        assertEquals(1L, token.epoch());
        assertTrue(next.epoch() > token.epoch());
    }

    @Test
    void repeatedNextCallsProduceAStrictlyMonotonicSequence() {
        FencingToken token = new FencingToken(1L);

        for (long expected = 2L; expected <= 10L; expected++) {
            token = token.next();
            assertEquals(expected, token.epoch());
        }
    }

    @Test
    void zeroAndNegativeEpochsAreRejected() {
        assertEquals("epoch must be positive",
                assertThrows(IllegalArgumentException.class, () -> new FencingToken(0L)).getMessage());
        assertThrows(IllegalArgumentException.class, () -> new FencingToken(-1L));
        assertThrows(IllegalArgumentException.class, () -> new FencingToken(Long.MIN_VALUE));
    }

    @Test
    void tokensWithTheSameEpochAreEqualSoLeaseChecksCanCompareThemByValue() {
        assertEquals(new FencingToken(7L), new FencingToken(7L));
        assertEquals(new FencingToken(7L).hashCode(), new FencingToken(7L).hashCode());
        assertTrue(new FencingToken(7L).toString().contains("7"));
    }
}
