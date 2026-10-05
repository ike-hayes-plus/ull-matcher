package io.github.ike.ullmatcher.ha.failover;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.state.ReplicaState;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The controller decides whether a failover happens at all and who wins it. The dangerous mistakes
 * are promoting a standby that is behind, promoting while the primary is still alive, and picking a
 * different winner on two nodes that see the same state - so candidate selection is pinned exactly.
 */
final class QuorumFailoverControllerTest {
    private static final long HEARTBEAT_TIMEOUT_NANOS = 1_000L;
    private final QuorumFailoverController controller = new QuorumFailoverController();

    @Test
    void aHealthyPrimaryIsKeptAndItsEpochIsEchoedBack() {
        FailoverDecision decision = controller.evaluate(
                primary(true, true, 1_000L, 100L, 5L),
                List.of(standby("node-b", 100L)),
                policy(0L, 1),
                1_500L);

        assertEquals(FailoverAction.KEEP_PRIMARY, decision.action());
        assertEquals("primary healthy", decision.reason());
        assertNull(decision.candidateNodeId());
        assertEquals(new FencingToken(5L), decision.nextToken());
    }

    @Test
    void theHeartbeatTimeoutIsInclusiveSoOneNanosecondDecidesFailover() {
        ReplicaState livePrimary = primary(true, true, 1_000L, 100L, 1L);
        List<ReplicaState> standbys = List.of(standby("node-b", 100L));

        FailoverDecision atTimeout = controller.evaluate(livePrimary, standbys, policy(0L, 1),
                1_000L + HEARTBEAT_TIMEOUT_NANOS);
        FailoverDecision pastTimeout = controller.evaluate(livePrimary, standbys, policy(0L, 1),
                1_000L + HEARTBEAT_TIMEOUT_NANOS + 1L);

        assertEquals(FailoverAction.KEEP_PRIMARY, atTimeout.action());
        assertEquals(FailoverAction.PROMOTE_STANDBY, pastTimeout.action());
    }

    @Test
    void anUnreachableOrUnhealthyPrimaryTriggersFailoverWithANewEpoch() {
        FailoverDecision unreachable = controller.evaluate(
                primary(false, true, 1_000L, 100L, 5L), List.of(standby("node-b", 100L)), policy(0L, 1), 1_100L);
        FailoverDecision unhealthy = controller.evaluate(
                primary(true, false, 1_000L, 100L, 5L), List.of(standby("node-b", 100L)), policy(0L, 1), 1_100L);

        assertEquals(FailoverAction.PROMOTE_STANDBY, unreachable.action());
        assertEquals("node-b", unreachable.candidateNodeId());
        assertEquals(new FencingToken(6L), unreachable.nextToken());
        assertEquals("primary unavailable and standby caught up", unreachable.reason());
        assertEquals(FailoverAction.PROMOTE_STANDBY, unhealthy.action());
    }

    @Test
    void tooFewStandbysHoldsBeforeTheHealthCheckEvenRuns() {
        FailoverDecision decision = controller.evaluate(
                primary(false, false, 1_000L, 100L, 5L),
                List.of(standby("node-b", 100L)),
                policy(0L, 2),
                9_000L);

        assertEquals(FailoverAction.HOLD, decision.action());
        assertEquals("insufficient standby replicas", decision.reason());
        assertNull(decision.candidateNodeId());
        assertEquals(new FencingToken(5L), decision.nextToken());
    }

    @Test
    void aLaggingStandbyIsNotPromoted() {
        FailoverDecision decision = controller.evaluate(
                primary(false, false, 1_000L, 100L, 5L),
                List.of(standby("node-b", 94L)),
                policy(5L, 1),
                9_000L);

        assertEquals(FailoverAction.HOLD, decision.action());
        assertEquals("no standby satisfies promotion lag and health checks", decision.reason());
    }

    @Test
    void aStandbyExactlyAtTheLagLimitIsStillPromotable() {
        FailoverDecision decision = controller.evaluate(
                primary(false, false, 1_000L, 100L, 5L),
                List.of(standby("node-b", 95L)),
                policy(5L, 1),
                9_000L);

        assertEquals(FailoverAction.PROMOTE_STANDBY, decision.action());
        assertEquals("node-b", decision.candidateNodeId());
    }

    @Test
    void unreachableAndUnhealthyStandbysAreIneligible() {
        FailoverDecision decision = controller.evaluate(
                primary(false, false, 1_000L, 100L, 5L),
                List.of(
                        replica("node-b", HaRole.STANDBY, false, true, 100L),
                        replica("node-c", HaRole.STANDBY, true, false, 100L),
                        replica("node-d", HaRole.PRIMARY, true, true, 100L),
                        replica("node-e", HaRole.FENCED, true, true, 100L)),
                policy(0L, 1),
                9_000L);

        assertEquals(FailoverAction.HOLD, decision.action());
        assertEquals("no standby satisfies promotion lag and health checks", decision.reason());
    }

    @Test
    void aCatchingUpReplicaIsEligibleOnceItHasCaughtUp() {
        FailoverDecision decision = controller.evaluate(
                primary(false, false, 1_000L, 100L, 5L),
                List.of(replica("node-b", HaRole.CATCHING_UP, true, true, 100L)),
                policy(0L, 1),
                9_000L);

        assertEquals(FailoverAction.PROMOTE_STANDBY, decision.action());
        assertEquals("node-b", decision.candidateNodeId());
    }

    @Test
    void theLeastLaggingStandbyWins() {
        FailoverDecision decision = controller.evaluate(
                primary(false, false, 1_000L, 100L, 5L),
                List.of(standby("node-b", 90L), standby("node-c", 99L), standby("node-d", 95L)),
                policy(20L, 1),
                9_000L);

        assertEquals("node-c", decision.candidateNodeId());
    }

    @Test
    void equalWatermarksAreBrokenByDurableThenAppliedThenReceivedThenNodeId() {
        ReplicaState deadPrimary = primary(false, false, 1_000L, 100L, 5L);
        ReplicaState lowerDurable = new ReplicaState("node-b", HaRole.STANDBY, true, true, 1_000L,
                new FencingToken(1L), new ReplicationCursor(100L, 100L, 90L, 0L));
        ReplicaState higherDurable = new ReplicaState("node-c", HaRole.STANDBY, true, true, 1_000L,
                new FencingToken(1L), new ReplicationCursor(100L, 120L, 90L, 0L));

        FailoverDecision byDurable = controller.evaluate(
                deadPrimary, List.of(lowerDurable, higherDurable), policy(20L, 1), 9_000L);

        assertEquals("node-c", byDurable.candidateNodeId(), "the more durable standby wins the tie");

        ReplicaState identicalA = new ReplicaState("node-z", HaRole.STANDBY, true, true, 1_000L,
                new FencingToken(1L), new ReplicationCursor(100L, 100L, 100L, 0L));
        ReplicaState identicalB = new ReplicaState("node-a", HaRole.STANDBY, true, true, 1_000L,
                new FencingToken(1L), new ReplicationCursor(100L, 100L, 100L, 0L));

        FailoverDecision byNodeId = controller.evaluate(
                deadPrimary, List.of(identicalA, identicalB), policy(20L, 1), 9_000L);

        assertEquals("node-a", byNodeId.candidateNodeId(),
                "fully tied standbys must be ordered deterministically so every observer agrees");
    }

    @Test
    void aStandbyAheadOfTheDeadPrimaryHasZeroLag() {
        FailoverDecision decision = controller.evaluate(
                primary(false, false, 1_000L, 10L, 5L),
                List.of(standby("node-b", 500L)),
                policy(0L, 1),
                9_000L);

        assertEquals(FailoverAction.PROMOTE_STANDBY, decision.action());
        assertEquals("node-b", decision.candidateNodeId());
    }

    @Test
    void invalidInputIsRejected() {
        ReplicaState aPrimary = primary(true, true, 1_000L, 100L, 5L);
        List<ReplicaState> standbys = List.of(standby("node-b", 100L));
        FailoverPolicy aPolicy = policy(0L, 1);

        assertThrows(NullPointerException.class, () -> controller.evaluate(null, standbys, aPolicy, 1L));
        assertThrows(NullPointerException.class, () -> controller.evaluate(aPrimary, null, aPolicy, 1L));
        assertThrows(NullPointerException.class, () -> controller.evaluate(aPrimary, standbys, null, 1L));
        assertThrows(IllegalArgumentException.class, () -> controller.evaluate(aPrimary, standbys, aPolicy, -1L));
        assertEquals("primary replica must have PRIMARY role",
                assertThrows(IllegalArgumentException.class, () -> controller.evaluate(
                        replica("node-a", HaRole.STANDBY, true, true, 100L), standbys, aPolicy, 1L)).getMessage());
    }

    private static FailoverPolicy policy(long maxPromotionLag, int minStandbyReplicas) {
        return new FailoverPolicy(HEARTBEAT_TIMEOUT_NANOS, maxPromotionLag, minStandbyReplicas);
    }

    private static ReplicaState primary(boolean reachable, boolean healthy, long lastHeartbeatNanos,
                                        long watermark, long epoch) {
        return new ReplicaState("node-a", HaRole.PRIMARY, reachable, healthy, lastHeartbeatNanos,
                new FencingToken(epoch), new ReplicationCursor(watermark, watermark, watermark, watermark));
    }

    private static ReplicaState standby(String nodeId, long watermark) {
        return replica(nodeId, HaRole.STANDBY, true, true, watermark);
    }

    private static ReplicaState replica(String nodeId, HaRole role, boolean reachable,
                                        boolean healthy, long watermark) {
        return new ReplicaState(nodeId, role, reachable, healthy, 1_000L, new FencingToken(1L),
                new ReplicationCursor(watermark, watermark, watermark, watermark));
    }
}
