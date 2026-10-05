package io.github.ike.ullmatcher.ha.failover;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Only a promotion decision may name a candidate; keep and hold must leave it null so a caller can
 * never accidentally promote on the back of a "nothing to do" verdict.
 */
final class FailoverDecisionTest {
    private static final FencingToken CURRENT = new FencingToken(4L);

    @Test
    void keepCarriesTheCurrentEpochAndNoCandidate() {
        FailoverDecision decision = FailoverDecision.keep(CURRENT, "primary healthy");

        assertEquals(FailoverAction.KEEP_PRIMARY, decision.action());
        assertEquals("primary healthy", decision.reason());
        assertNull(decision.candidateNodeId());
        assertEquals(CURRENT, decision.nextToken());
    }

    @Test
    void holdCarriesTheCurrentEpochAndNoCandidate() {
        FailoverDecision decision = FailoverDecision.hold(CURRENT, "insufficient standby replicas");

        assertEquals(FailoverAction.HOLD, decision.action());
        assertEquals("insufficient standby replicas", decision.reason());
        assertNull(decision.candidateNodeId());
        assertEquals(CURRENT, decision.nextToken());
    }

    @Test
    void promoteCarriesTheCandidateAndTheNextEpoch() {
        FailoverDecision decision = FailoverDecision.promote("node-b", CURRENT.next(), "primary unavailable");

        assertEquals(FailoverAction.PROMOTE_STANDBY, decision.action());
        assertEquals("node-b", decision.candidateNodeId());
        assertEquals(new FencingToken(5L), decision.nextToken());
        assertTrue(decision.nextToken().epoch() > CURRENT.epoch());
    }

    @Test
    void decisionsWithTheSameVerdictAreEqual() {
        FailoverDecision first = FailoverDecision.promote("node-b", CURRENT, "reason");
        FailoverDecision second = new FailoverDecision(FailoverAction.PROMOTE_STANDBY, "reason", "node-b", CURRENT);

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertTrue(first.toString().contains("node-b"));
    }

    @Test
    void everyActionIsReachableThroughAFactory() {
        assertEquals(3, FailoverAction.values().length);
        assertEquals(FailoverAction.KEEP_PRIMARY, FailoverDecision.keep(CURRENT, "r").action());
        assertEquals(FailoverAction.HOLD, FailoverDecision.hold(CURRENT, "r").action());
        assertEquals(FailoverAction.PROMOTE_STANDBY, FailoverDecision.promote("n", CURRENT, "r").action());
    }
}
