package io.github.ike.ullmatcher.ha.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

final class TransportSecuritySnapshotTest {
    @Test
    void noneDescribesATransportThatNeverReloadedCredentials() {
        TransportSecuritySnapshot snapshot = TransportSecuritySnapshot.none();

        assertEquals(0L, snapshot.generation());
        assertEquals(0L, snapshot.reloadCount());
        assertEquals(0L, snapshot.failureCount());
        assertFalse(snapshot.reloading());
        assertEquals("", snapshot.lastError());
    }

    @Test
    void aFailedReloadIsDistinguishableFromTheInitialState() {
        TransportSecuritySnapshot failed = new TransportSecuritySnapshot(3L, 2L, 1L, false, "keystore unreadable");

        assertNotEquals(TransportSecuritySnapshot.none(), failed);
        assertEquals(1L, failed.failureCount());
        assertEquals("keystore unreadable", failed.lastError());
    }

    @Test
    void snapshotsWithTheSameStateAreEqual() {
        TransportSecuritySnapshot first = new TransportSecuritySnapshot(1L, 1L, 0L, true, "");
        TransportSecuritySnapshot second = new TransportSecuritySnapshot(1L, 1L, 0L, true, "");

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }
}
