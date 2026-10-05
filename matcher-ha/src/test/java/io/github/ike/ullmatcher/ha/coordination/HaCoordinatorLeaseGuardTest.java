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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers what the coordinator does when the lease store disagrees with the failover decision: a
 * renewal that silently fails, a promotion that loses the acquire race, and a cluster with no lease
 * recorded yet. In all three cases the coordinator must choose the conservative outcome.
 */
final class HaCoordinatorLeaseGuardTest {
    private static final long LEASE_TTL_NANOS = 5_000L;

    @Test
    void aPrimaryThatCannotRenewFencesItselfEvenThoughItStillHoldsTheLease() {
        FixedLeaseStore leaseStore = new FixedLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 10_000L), false, false);
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", newLoop(), HaRole.PRIMARY, new FencingToken(1L));
        HaCoordinator coordinator = coordinator("node-a", runtime, leaseStore, FailoverPolicy.defaults());

        HaTickResult result = coordinator.tick(
                primary("node-a", true, true, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);

        assertEquals(FailoverAction.HOLD, result.action());
        assertEquals(HaRole.PRIMARY, result.roleBefore());
        assertEquals(HaRole.FENCED, result.roleAfter());
        assertFalse(result.leaseChanged());
        assertEquals("primary failed to renew lease and was fenced", result.reason());
        assertFalse(runtime.acceptsClientCommands());
    }

    @Test
    void aStandbyThatLosesTheAcquireRaceHoldsInsteadOfPromoting() {
        FixedLeaseStore leaseStore = new FixedLeaseStore(
                new ClusterLease("node-x", new FencingToken(4L), 10_000L), false, false);
        HaMatchRuntime runtime = new HaMatchRuntime("node-b", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        HaCoordinator coordinator = coordinator("node-b", runtime, leaseStore, new FailoverPolicy(500L, 0L, 1));

        HaTickResult result = coordinator.tick(
                primary("node-a", false, false, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);

        assertEquals(FailoverAction.HOLD, result.action());
        assertEquals(HaRole.STANDBY, result.roleAfter());
        assertFalse(result.leaseChanged());
        assertEquals("failed to acquire lease for promotion", result.reason());
        assertFalse(runtime.acceptsClientCommands(), "a failed acquire must not open the loop");
        assertEquals("node-x", leaseStore.currentLease().ownerNodeId());
    }

    @Test
    void withNoLeaseRecordedYetTheProposedEpochIsUsedAsIs() {
        FixedLeaseStore leaseStore = new FixedLeaseStore(null, true, true);
        HaMatchRuntime runtime = new HaMatchRuntime("node-b", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        HaCoordinator coordinator = coordinator("node-b", runtime, leaseStore, new FailoverPolicy(500L, 0L, 1));

        HaTickResult result = coordinator.tick(
                primary("node-a", false, false, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);

        assertEquals(FailoverAction.PROMOTE_STANDBY, result.action());
        assertEquals(HaRole.PRIMARY, result.roleAfter());
        assertTrue(result.leaseChanged());
        assertEquals(2L, leaseStore.acquiredToken.epoch(),
                "the proposed epoch is the observed primary epoch plus one");
        assertEquals(new FencingToken(2L), runtime.fencingToken());
    }

    @Test
    void anEpochBehindTheRecordedLeaseIsLiftedAboveItBeforePromoting() {
        FixedLeaseStore leaseStore = new FixedLeaseStore(
                new ClusterLease("node-a", new FencingToken(9L), 1_000L), true, true);
        HaMatchRuntime runtime = new HaMatchRuntime("node-b", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        HaCoordinator coordinator = coordinator("node-b", runtime, leaseStore, new FailoverPolicy(500L, 0L, 1));

        HaTickResult result = coordinator.tick(
                primary("node-a", false, false, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);

        assertEquals(FailoverAction.PROMOTE_STANDBY, result.action());
        assertEquals(10L, leaseStore.acquiredToken.epoch(),
                "a stale observed epoch must not let two primaries reuse the same fencing token");
    }

    @Test
    void aHoldDecisionIsPassedThroughWithoutTouchingTheLease() {
        FixedLeaseStore leaseStore = new FixedLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 10_000L), false, false);
        HaMatchRuntime runtime = new HaMatchRuntime("node-b", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        HaCoordinator coordinator = coordinator("node-b", runtime, leaseStore, new FailoverPolicy(500L, 0L, 2));

        HaTickResult result = coordinator.tick(
                primary("node-a", false, false, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);

        assertEquals(FailoverAction.HOLD, result.action());
        assertEquals(HaRole.STANDBY, result.roleAfter());
        assertEquals("insufficient standby replicas", result.reason());
        assertNull(leaseStore.acquiredToken, "a HOLD must never reach the lease store");
    }

    @Test
    void negativeClockReadingsAreRejected() {
        FixedLeaseStore leaseStore = new FixedLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 10_000L), false, false);
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", newLoop(), HaRole.PRIMARY, new FencingToken(1L));
        HaCoordinator coordinator = coordinator("node-a", runtime, leaseStore, FailoverPolicy.defaults());
        ReplicaState observedPrimary = primary("node-a", true, true, 1_000L, 100L);
        List<ReplicaState> standbys = List.of(standby("node-b", 100L));

        assertThrows(IllegalArgumentException.class, () -> coordinator.tick(observedPrimary, standbys, -1L));
        assertThrows(NullPointerException.class, () -> coordinator.tick(null, standbys, 1_000L));
        assertThrows(NullPointerException.class, () -> coordinator.tick(observedPrimary, null, 1_000L));
    }

    @Test
    void constructorRejectsMissingCollaboratorsAndNonPositiveTtl() {
        FixedLeaseStore leaseStore = new FixedLeaseStore(null, true, true);
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        QuorumFailoverController controller = new QuorumFailoverController();
        FailoverPolicy policy = FailoverPolicy.defaults();

        assertThrows(NullPointerException.class,
                () -> new HaCoordinator(null, runtime, leaseStore, controller, policy, LEASE_TTL_NANOS));
        assertThrows(NullPointerException.class,
                () -> new HaCoordinator("node-a", null, leaseStore, controller, policy, LEASE_TTL_NANOS));
        assertThrows(NullPointerException.class,
                () -> new HaCoordinator("node-a", runtime, null, controller, policy, LEASE_TTL_NANOS));
        assertThrows(NullPointerException.class,
                () -> new HaCoordinator("node-a", runtime, leaseStore, null, policy, LEASE_TTL_NANOS));
        assertThrows(NullPointerException.class,
                () -> new HaCoordinator("node-a", runtime, leaseStore, controller, null, LEASE_TTL_NANOS));
        assertThrows(IllegalArgumentException.class,
                () -> new HaCoordinator("node-a", runtime, leaseStore, controller, policy, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new HaCoordinator("node-a", runtime, leaseStore, controller, policy, -1L));
    }

    private static HaCoordinator coordinator(String nodeId, HaMatchRuntime runtime,
                                             LeaseStore leaseStore, FailoverPolicy policy) {
        return new HaCoordinator(nodeId, runtime, leaseStore, new QuorumFailoverController(),
                policy, LEASE_TTL_NANOS);
    }

    private static ReplicaState primary(String nodeId, boolean reachable, boolean healthy,
                                        long lastHeartbeatNanos, long applied) {
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

    /**
     * Lease store whose acquire and extend outcomes are fixed up front, so the coordinator can be
     * driven through store responses that a healthy in-memory store would never produce.
     */
    private static final class FixedLeaseStore implements LeaseStore {
        private final ClusterLease lease;
        private final boolean acquireSucceeds;
        private final boolean leaseIsMine;
        private FencingToken acquiredToken;

        private FixedLeaseStore(ClusterLease lease, boolean acquireSucceeds, boolean leaseIsMine) {
            this.lease = lease;
            this.acquireSucceeds = acquireSucceeds;
            this.leaseIsMine = leaseIsMine;
        }

        @Override
        public ClusterLease currentLease() {
            return lease;
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (!acquireSucceeds) {
                return false;
            }
            acquiredToken = fencingToken;
            return true;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            return false;
        }

        @Override
        public boolean isHeldBy(String nodeId, FencingToken fencingToken, long nowNanos) {
            return leaseIsMine || LeaseStore.super.isHeldBy(nodeId, fencingToken, nowNanos);
        }
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }
}
