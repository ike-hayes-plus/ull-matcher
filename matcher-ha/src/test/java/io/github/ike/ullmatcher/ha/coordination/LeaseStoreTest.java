package io.github.ike.ullmatcher.ha.coordination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code isHeldBy} is the ownership check a primary runs before every batch, so each way of losing
 * ownership - no lease, an expired lease, a different owner, a superseded epoch - must answer false.
 */
final class LeaseStoreTest {
    private static final long NOW_NANOS = 2_000L;

    @Test
    void ownershipRequiresTheExactOwnerAndEpochOnAnUnexpiredLease() {
        LeaseStore store = storeOf(new ClusterLease("node-a", new FencingToken(3L), 10_000L));

        assertTrue(store.isHeldBy("node-a", new FencingToken(3L), NOW_NANOS));
    }

    @Test
    void anAbsentLeaseIsHeldByNobody() {
        LeaseStore store = storeOf(null);

        assertFalse(store.isHeldBy("node-a", new FencingToken(3L), NOW_NANOS));
    }

    @Test
    void anExpiredLeaseIsNoLongerHeldByItsFormerOwner() {
        LeaseStore store = storeOf(new ClusterLease("node-a", new FencingToken(3L), 2_000L));

        assertFalse(store.isHeldBy("node-a", new FencingToken(3L), NOW_NANOS));
    }

    @Test
    void aDifferentOwnerDoesNotHoldTheLease() {
        LeaseStore store = storeOf(new ClusterLease("node-a", new FencingToken(3L), 10_000L));

        assertFalse(store.isHeldBy("node-b", new FencingToken(3L), NOW_NANOS));
    }

    @Test
    void aSupersededEpochDoesNotHoldTheLeaseEvenForTheSameOwner() {
        LeaseStore store = storeOf(new ClusterLease("node-a", new FencingToken(3L), 10_000L));

        assertFalse(store.isHeldBy("node-a", new FencingToken(2L), NOW_NANOS),
                "an old epoch must not pass the fencing check");
        assertFalse(store.isHeldBy("node-a", new FencingToken(4L), NOW_NANOS),
                "an epoch the store never granted must not pass either");
    }

    private static LeaseStore storeOf(ClusterLease lease) {
        return new LeaseStore() {
            @Override
            public ClusterLease currentLease() {
                return lease;
            }

            @Override
            public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
                throw new UnsupportedOperationException("not used by isHeldBy");
            }

            @Override
            public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
                throw new UnsupportedOperationException("not used by isHeldBy");
            }
        };
    }
}
