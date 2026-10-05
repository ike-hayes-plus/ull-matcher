package io.github.ike.ullmatcher.ha.failover;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FailoverPolicyTest {
    @Test
    void defaultsWaitThreeSecondsAndRequireALosslessCaughtUpStandby() {
        FailoverPolicy policy = FailoverPolicy.defaults();

        assertEquals(TimeUnit.SECONDS.toNanos(3), policy.primaryHeartbeatTimeoutNanos());
        assertEquals(0L, policy.maxPromotionLag(), "the default must not promote a standby that is behind");
        assertEquals(1, policy.minStandbyReplicas());
    }

    @Test
    void aZeroOrNegativeHeartbeatTimeoutIsRejectedBecauseItWouldFailOverInstantly() {
        assertEquals("primaryHeartbeatTimeoutNanos must be positive",
                assertThrows(IllegalArgumentException.class, () -> new FailoverPolicy(0L, 0L, 1)).getMessage());
        assertThrows(IllegalArgumentException.class, () -> new FailoverPolicy(-1L, 0L, 1));
    }

    @Test
    void aNegativePromotionLagIsRejectedButZeroIsTheStrictestValidSetting() {
        assertEquals(0L, new FailoverPolicy(1L, 0L, 1).maxPromotionLag());

        assertEquals("maxPromotionLag must be non-negative",
                assertThrows(IllegalArgumentException.class, () -> new FailoverPolicy(1L, -1L, 1)).getMessage());
    }

    @Test
    void requiringZeroStandbysIsRejectedBecauseFailoverWouldHaveNoTarget() {
        assertEquals("minStandbyReplicas must be positive",
                assertThrows(IllegalArgumentException.class, () -> new FailoverPolicy(1L, 0L, 0)).getMessage());
        assertThrows(IllegalArgumentException.class, () -> new FailoverPolicy(1L, 0L, -1));
    }

    @Test
    void policiesWithTheSameThresholdsAreEqual() {
        assertEquals(new FailoverPolicy(1L, 2L, 3), new FailoverPolicy(1L, 2L, 3));
        assertEquals(new FailoverPolicy(1L, 2L, 3).hashCode(), new FailoverPolicy(1L, 2L, 3).hashCode());
    }
}
