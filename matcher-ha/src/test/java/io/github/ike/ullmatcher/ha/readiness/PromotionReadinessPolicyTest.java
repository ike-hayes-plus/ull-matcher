package io.github.ike.ullmatcher.ha.readiness;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PromotionReadinessPolicyTest {
    @Test
    void strictPolicyToleratesNoLagAtAll() {
        PromotionReadinessPolicy policy = PromotionReadinessPolicy.strict();

        assertEquals(0L, policy.maxReceivedLag());
        assertEquals(0L, policy.maxDurableLag());
        assertEquals(0L, policy.maxAppliedLag());
        assertEquals(0L, policy.maxSnapshotLag());
        assertEquals(new PromotionReadinessPolicy(0L, 0L, 0L, 0L), policy);
    }

    @Test
    void zeroThresholdsAreAcceptedButNegativeOnesAreNot() {
        assertEquals(0L, new PromotionReadinessPolicy(0L, 0L, 0L, 0L).maxReceivedLag());

        assertThrows(IllegalArgumentException.class, () -> new PromotionReadinessPolicy(-1L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new PromotionReadinessPolicy(0L, -1L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new PromotionReadinessPolicy(0L, 0L, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new PromotionReadinessPolicy(0L, 0L, 0L, -1L));
    }

    @Test
    void policiesWithDifferentThresholdsAreNotEqual() {
        PromotionReadinessPolicy relaxed = new PromotionReadinessPolicy(1L, 2L, 3L, 4L);

        assertNotEquals(PromotionReadinessPolicy.strict(), relaxed);
        assertEquals(new PromotionReadinessPolicy(1L, 2L, 3L, 4L).hashCode(), relaxed.hashCode());
        assertTrue(relaxed.toString().contains("maxSnapshotLag=4"));
    }
}
