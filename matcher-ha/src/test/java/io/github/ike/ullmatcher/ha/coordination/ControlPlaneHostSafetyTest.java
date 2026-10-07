package io.github.ike.ullmatcher.ha.coordination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ControlPlaneHostSafetyTest {
    @Test
    void loopbackConnectStringIsAcceptedInProduction() {
        assertDoesNotThrow(() -> ControlPlaneHostSafety.requireLoopbackConnectStringInProduction(
                "127.0.0.1:2181,localhost:2182,[::1]:2183/chroot", "ZooKeeper"));
    }

    @Test
    void remoteConnectStringIsRejectedInProduction() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> ControlPlaneHostSafety.requireLoopbackConnectStringInProduction(
                        "127.0.0.1:2181,10.0.0.8:2181", "ZooKeeper"));
        assertTrue(error.getMessage().contains("10.0.0.8"));
    }

    @Test
    void onlyExactLoopbackLiteralsCount() {
        assertTrue(ControlPlaneHostSafety.isLoopbackHost("127.0.0.1"));
        assertTrue(ControlPlaneHostSafety.isLoopbackHost("localhost"));
        assertTrue(ControlPlaneHostSafety.isLoopbackHost("[::1]"));
        assertFalse(ControlPlaneHostSafety.isLoopbackHost("127.0.0.2"));
        assertFalse(ControlPlaneHostSafety.isLoopbackHost("0.0.0.0"));
    }

    @Test
    void parsesHostsAndStripsChroot() {
        assertArrayEquals(
                new String[] {"127.0.0.1", "localhost"},
                ControlPlaneHostSafety.hostsOf("127.0.0.1:2181, localhost:2182/ull")
        );
    }
}
