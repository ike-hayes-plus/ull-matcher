package io.github.ike.ullmatcher.ha.coordination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lease expiry is the single boundary that decides when a standby may take over, so the exact
 * instant of expiry matters: the lease must be considered gone at {@code expiresAtNanos}, not after.
 */
final class ClusterLeaseTest {
    @Test
    void aLeaseIsExpiredExactlyAtItsDeadline() {
        ClusterLease lease = new ClusterLease("node-a", new FencingToken(1L), 1_000L);

        assertFalse(lease.isExpired(999L));
        assertTrue(lease.isExpired(1_000L), "the deadline instant itself must count as expired");
        assertTrue(lease.isExpired(1_001L));
    }

    @Test
    void aLeaseThatExpiresAtZeroIsAlreadyExpired() {
        ClusterLease lease = new ClusterLease("node-a", new FencingToken(1L), 0L);

        assertTrue(lease.isExpired(0L));
    }

    @Test
    void aNegativeClockReadingIsRejectedInsteadOfSilentlyExtendingTheLease() {
        ClusterLease lease = new ClusterLease("node-a", new FencingToken(1L), 1_000L);

        assertThrows(IllegalArgumentException.class, () -> lease.isExpired(-1L));
    }

    @Test
    void leaseConstructionValidatesOwnerTokenAndDeadline() {
        assertThrows(NullPointerException.class, () -> new ClusterLease(null, new FencingToken(1L), 1L));
        assertThrows(NullPointerException.class, () -> new ClusterLease("node-a", null, 1L));
        assertThrows(IllegalArgumentException.class, () -> new ClusterLease("node-a", new FencingToken(1L), -1L));
    }

    @Test
    void leasesForTheSameOwnerAndEpochAreEqual() {
        ClusterLease first = new ClusterLease("node-a", new FencingToken(2L), 5_000L);
        ClusterLease second = new ClusterLease("node-a", new FencingToken(2L), 5_000L);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertEquals("node-a", first.ownerNodeId());
        assertEquals(2L, first.fencingToken().epoch());
        assertEquals(5_000L, first.expiresAtNanos());
    }
}
