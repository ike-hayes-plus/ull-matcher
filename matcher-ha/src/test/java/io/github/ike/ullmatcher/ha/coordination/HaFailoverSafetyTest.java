package io.github.ike.ullmatcher.ha.coordination;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.failover.FailoverAction;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.failover.QuorumFailoverController;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.state.ReplicaState;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Safety properties of lease-based failover, driven by an injected clock rather than sleeps.
 * <p>
 * Every assertion here is about a property that must never be violated: fencing tokens only move
 * forward, at most one node owns the lease at a time, and a fenced node never serves clients again.
 */
final class HaFailoverSafetyTest {
    private static final long LEASE_TTL_NANOS = 5_000L;

    @Test
    void fencingEpochStrictlyIncreasesAcrossSuccessivePromotions() {
        TrackingLeaseStore leaseStore = new TrackingLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 1_000L));
        long nowNanos = 2_000L;

        for (int round = 0; round < 5; round++) {
            String nodeId = "node-" + round;
            HaMatchRuntime runtime = new HaMatchRuntime(nodeId, newLoop(), HaRole.STANDBY, new FencingToken(1L));
            HaCoordinator coordinator = new HaCoordinator(
                    nodeId, runtime, leaseStore, new QuorumFailoverController(),
                    new FailoverPolicy(500L, 0L, 1), LEASE_TTL_NANOS);

            // Expire the lease before each round so the next standby is eligible to take over.
            nowNanos = leaseStore.currentLease().expiresAtNanos() + 1L;
            coordinator.tick(primary("node-a", false, false, 1_000L, 100L), List.of(standby(nodeId, 100L)), nowNanos);
        }

        List<Long> epochs = leaseStore.grantedEpochs();
        assertEquals(5, epochs.size(), "every round must grant a lease");
        for (int index = 1; index < epochs.size(); index++) {
            assertTrue(epochs.get(index) > epochs.get(index - 1),
                    "epoch must increase: " + epochs);
        }
    }

    @Test
    void onlyOneOfTwoCompetingStandbysAcquiresTheLease() {
        TrackingLeaseStore leaseStore = new TrackingLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 1_000L));
        HaMatchRuntime runtimeB = new HaMatchRuntime("node-b", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        HaMatchRuntime runtimeC = new HaMatchRuntime("node-c", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        List<ReplicaState> standbys = List.of(standby("node-b", 100L), standby("node-c", 100L));
        ReplicaState deadPrimary = primary("node-a", false, false, 1_000L, 100L);

        HaTickResult resultB = coordinator("node-b", runtimeB, leaseStore)
                .tick(deadPrimary, standbys, 2_000L);
        HaTickResult resultC = coordinator("node-c", runtimeC, leaseStore)
                .tick(deadPrimary, standbys, 2_001L);

        assertEquals(FailoverAction.PROMOTE_STANDBY, resultB.action());
        assertEquals(HaRole.PRIMARY, resultB.roleAfter());
        assertNotEquals(HaRole.PRIMARY, resultC.roleAfter(), "second standby must not also become primary");
        assertFalse(runtimeC.acceptsClientCommands(), "second standby must not accept client commands");
        assertEquals("node-b", leaseStore.currentLease().ownerNodeId());
    }

    @Test
    void standbyWaitsForTheHeartbeatTimeoutBeforePromoting() {
        long heartbeatTimeoutNanos = 1_000L;
        long lastHeartbeatNanos = 1_000L;
        TrackingLeaseStore leaseStore = new TrackingLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 10_000L));
        HaMatchRuntime runtime = new HaMatchRuntime("node-b", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        HaCoordinator coordinator = new HaCoordinator(
                "node-b", runtime, leaseStore, new QuorumFailoverController(),
                new FailoverPolicy(heartbeatTimeoutNanos, 0L, 1), LEASE_TTL_NANOS);

        HaTickResult beforeTimeout = coordinator.tick(
                primary("node-a", true, true, lastHeartbeatNanos, 100L),
                List.of(standby("node-b", 100L)),
                lastHeartbeatNanos + heartbeatTimeoutNanos - 1L);

        assertNotEquals(FailoverAction.PROMOTE_STANDBY, beforeTimeout.action());
        assertEquals("node-a", leaseStore.currentLease().ownerNodeId());
        assertFalse(runtime.acceptsClientCommands());
    }

    @Test
    void fencedNodeNeverResumesServingClients() {
        TrackingLeaseStore leaseStore = new TrackingLeaseStore(
                new ClusterLease("node-b", new FencingToken(9L), 10_000L));
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", newLoop(), HaRole.PRIMARY, new FencingToken(2L));
        HaCoordinator coordinator = coordinator("node-a", runtime, leaseStore);

        HaTickResult fenced = coordinator.tick(
                primary("node-a", true, true, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);
        assertEquals(HaRole.FENCED, fenced.roleAfter());

        for (long tick = 3_000L; tick <= 9_000L; tick += 1_000L) {
            HaTickResult later = coordinator.tick(
                    primary("node-a", true, true, tick, 100L), List.of(standby("node-b", 100L)), tick);
            assertEquals(HaRole.FENCED, later.roleAfter(), "node must stay fenced at tick " + tick);
            assertFalse(runtime.acceptsClientCommands(), "fenced node must stay closed at tick " + tick);
        }
        assertEquals("node-b", leaseStore.currentLease().ownerNodeId());
    }

    @Test
    void leaseIsNotExtendedByANodeHoldingAStaleFencingToken() {
        TrackingLeaseStore leaseStore = new TrackingLeaseStore(
                new ClusterLease("node-a", new FencingToken(4L), 10_000L));

        assertFalse(leaseStore.tryExtend("node-a", new FencingToken(3L), 2_000L, LEASE_TTL_NANOS));
        assertFalse(leaseStore.tryExtend("node-b", new FencingToken(4L), 2_000L, LEASE_TTL_NANOS));
        assertTrue(leaseStore.tryExtend("node-a", new FencingToken(4L), 2_000L, LEASE_TTL_NANOS));
    }

    private static HaCoordinator coordinator(String nodeId, HaMatchRuntime runtime, LeaseStore leaseStore) {
        return new HaCoordinator(nodeId, runtime, leaseStore, new QuorumFailoverController(),
                new FailoverPolicy(500L, 0L, 1), LEASE_TTL_NANOS);
    }

    private static ReplicaState primary(String nodeId, boolean reachable, boolean healthy, long lastHeartbeatNanos, long applied) {
        return new ReplicaState(nodeId, HaRole.PRIMARY, reachable, healthy, lastHeartbeatNanos,
                new FencingToken(1L), new ReplicationCursor(applied, applied, applied, applied));
    }

    private static ReplicaState standby(String nodeId, long applied) {
        return new ReplicaState(nodeId, HaRole.STANDBY, true, true, 1_000L,
                new FencingToken(1L), new ReplicationCursor(applied, applied, applied, applied));
    }

    private static MatchLoop newLoop() {
        return new MatchLoop(
                new SpscRingBuffer<>(16),
                new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler()));
    }

    /** In-memory lease store that records every granted epoch so monotonicity can be asserted. */
    private static final class TrackingLeaseStore implements LeaseStore {
        private final List<Long> grantedEpochs = new ArrayList<>();
        private ClusterLease lease;

        private TrackingLeaseStore(ClusterLease lease) {
            this.lease = lease;
        }

        private List<Long> grantedEpochs() {
            return List.copyOf(grantedEpochs);
        }

        @Override
        public ClusterLease currentLease() {
            return lease;
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (lease != null && !lease.isExpired(nowNanos) && !lease.ownerNodeId().equals(nodeId)) {
                return false;
            }
            lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
            grantedEpochs.add(fencingToken.epoch());
            return true;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (lease != null && !lease.isExpired(nowNanos)
                    && lease.ownerNodeId().equals(nodeId)
                    && lease.fencingToken().equals(fencingToken)) {
                lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
                return true;
            }
            return false;
        }
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }
}
