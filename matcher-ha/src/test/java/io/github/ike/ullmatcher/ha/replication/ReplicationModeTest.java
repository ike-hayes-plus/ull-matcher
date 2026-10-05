package io.github.ike.ullmatcher.ha.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ack quorum is what makes a committed write survive losing the primary, so the arithmetic is
 * pinned per mode and per standby count rather than spot-checked.
 */
final class ReplicationModeTest {
    @Test
    void localOnlyNeverWaitsForAStandby() {
        for (int standbyCount = 0; standbyCount <= 5; standbyCount++) {
            assertEquals(0, ReplicationMode.LOCAL_ONLY.requiredAcks(standbyCount),
                    "LOCAL_ONLY must not require acks for " + standbyCount + " standbys");
        }
    }

    @Test
    void waitForAnyStandbyNeedsOneAckOnceAtLeastOneStandbyExists() {
        assertEquals(0, ReplicationMode.WAIT_FOR_ANY_STANDBY.requiredAcks(0));
        assertEquals(1, ReplicationMode.WAIT_FOR_ANY_STANDBY.requiredAcks(1));
        assertEquals(1, ReplicationMode.WAIT_FOR_ANY_STANDBY.requiredAcks(2));
        assertEquals(1, ReplicationMode.WAIT_FOR_ANY_STANDBY.requiredAcks(9));
    }

    @Test
    void quorumIsStrictMajorityOfStandbysAndZeroWhenThereAreNone() {
        assertEquals(0, ReplicationMode.WAIT_FOR_QUORUM_STANDBYS.requiredAcks(0));
        assertEquals(1, ReplicationMode.WAIT_FOR_QUORUM_STANDBYS.requiredAcks(1));
        assertEquals(2, ReplicationMode.WAIT_FOR_QUORUM_STANDBYS.requiredAcks(2));
        assertEquals(2, ReplicationMode.WAIT_FOR_QUORUM_STANDBYS.requiredAcks(3));
        assertEquals(3, ReplicationMode.WAIT_FOR_QUORUM_STANDBYS.requiredAcks(4));
        assertEquals(3, ReplicationMode.WAIT_FOR_QUORUM_STANDBYS.requiredAcks(5));
    }

    @Test
    void waitForAllStandbysRequiresEveryStandby() {
        for (int standbyCount = 0; standbyCount <= 5; standbyCount++) {
            assertEquals(standbyCount, ReplicationMode.WAIT_FOR_ALL_STANDBYS.requiredAcks(standbyCount));
        }
    }

    @Test
    void twoQuorumsAlwaysIntersectSoTwoPrimariesCannotBothCommit() {
        for (int standbyCount = 1; standbyCount <= 9; standbyCount++) {
            int required = ReplicationMode.WAIT_FOR_QUORUM_STANDBYS.requiredAcks(standbyCount);
            assertTrue(required * 2 > standbyCount,
                    "quorum of " + required + " does not intersect itself for " + standbyCount + " standbys");
        }
    }

    @Test
    void noModeEverRequiresMoreAcksThanThereAreStandbys() {
        for (ReplicationMode mode : ReplicationMode.values()) {
            for (int standbyCount = 0; standbyCount <= 8; standbyCount++) {
                int required = mode.requiredAcks(standbyCount);
                assertTrue(required >= 0 && required <= standbyCount,
                        mode + " requires " + required + " acks from " + standbyCount + " standbys");
            }
        }
    }
}
