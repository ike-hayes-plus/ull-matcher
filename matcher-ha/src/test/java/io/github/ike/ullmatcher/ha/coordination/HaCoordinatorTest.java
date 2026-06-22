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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class HaCoordinatorTest {
    @Test
    void primaryRenewsLeaseWhenHealthy() {
        InMemoryLeaseStore leaseStore = new InMemoryLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 10_000L)
        );
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", newLoop(), HaRole.PRIMARY, new FencingToken(1L));
        HaCoordinator coordinator = new HaCoordinator(
                "node-a", runtime, leaseStore, new QuorumFailoverController(), FailoverPolicy.defaults(), 5_000L
        );

        HaTickResult result = coordinator.tick(primary("node-a", true, true, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);

        assertEquals(FailoverAction.KEEP_PRIMARY, result.action());
        assertTrue(result.leaseChanged());
        assertEquals(HaRole.PRIMARY, result.roleAfter());
        assertEquals(7_000L, leaseStore.currentLease().expiresAtNanos());
    }

    @Test
    void standbyPromotesAfterAcquiringExpiredLease() {
        InMemoryLeaseStore leaseStore = new InMemoryLeaseStore(
                new ClusterLease("node-a", new FencingToken(1L), 1_000L)
        );
        HaMatchRuntime runtime = new HaMatchRuntime("node-b", newLoop(), HaRole.STANDBY, new FencingToken(1L));
        HaCoordinator coordinator = new HaCoordinator(
                "node-b", runtime, leaseStore, new QuorumFailoverController(),
                new FailoverPolicy(500L, 0L, 1), 5_000L
        );

        HaTickResult result = coordinator.tick(
                primary("node-a", false, false, 1_000L, 100L),
                List.of(standby("node-b", 100L)),
                2_000L
        );

        assertEquals(FailoverAction.PROMOTE_STANDBY, result.action());
        assertEquals(HaRole.PRIMARY, result.roleAfter());
        assertTrue(runtime.acceptsClientCommands());
        assertEquals("node-b", leaseStore.currentLease().ownerNodeId());
        assertEquals(2L, leaseStore.currentLease().fencingToken().epoch());
    }

    @Test
    @Tag("chaos")
    void primaryFencesItselfAfterLeaseLoss() {
        InMemoryLeaseStore leaseStore = new InMemoryLeaseStore(
                new ClusterLease("node-b", new FencingToken(3L), 10_000L)
        );
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", newLoop(), HaRole.PRIMARY, new FencingToken(2L));
        HaCoordinator coordinator = new HaCoordinator(
                "node-a", runtime, leaseStore, new QuorumFailoverController(), FailoverPolicy.defaults(), 5_000L
        );

        HaTickResult result = coordinator.tick(primary("node-a", true, true, 1_000L, 100L), List.of(standby("node-b", 100L)), 2_000L);

        assertEquals(HaRole.FENCED, result.roleAfter());
        assertFalse(runtime.acceptsClientCommands());
        assertEquals(FailoverAction.HOLD, result.action());
    }

    private static ReplicaState primary(String nodeId, boolean reachable, boolean healthy, long lastHeartbeatNanos, long applied) {
        return new ReplicaState(
                nodeId,
                HaRole.PRIMARY,
                reachable,
                healthy,
                lastHeartbeatNanos,
                new FencingToken(1L),
                new ReplicationCursor(applied, applied, applied, applied)
        );
    }

    private static ReplicaState standby(String nodeId, long applied) {
        return new ReplicaState(
                nodeId,
                HaRole.STANDBY,
                true,
                true,
                1_000L,
                new FencingToken(1L),
                new ReplicationCursor(applied, applied, applied, applied)
        );
    }

    private static MatchLoop newLoop() {
        return new MatchLoop(
                new SpscRingBuffer<>(16),
                new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler())
        );
    }

    private static final class InMemoryLeaseStore implements LeaseStore {
        private ClusterLease lease;

        private InMemoryLeaseStore(ClusterLease lease) {
            this.lease = lease;
        }

        @Override
        public ClusterLease currentLease() {
            return lease;
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (lease == null || lease.isExpired(nowNanos) || lease.ownerNodeId().equals(nodeId)) {
                lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
                return true;
            }
            return false;
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
