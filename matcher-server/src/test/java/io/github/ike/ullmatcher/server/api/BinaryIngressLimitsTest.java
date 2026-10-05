/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BinaryIngressLimitsTest {
    @Test
    void defaultsEnableIdleReaping() {
        BinaryIngressLimits limits = BinaryIngressLimits.defaults();

        assertEquals(BinaryIngressLimits.DEFAULT_MAX_CONNECTIONS, limits.maxConnections());
        assertTrue(limits.idleTimeoutEnabled());
    }

    @Test
    void zeroIdleTimeoutDisablesReaping() {
        BinaryIngressLimits limits = new BinaryIngressLimits(8, 1_000L, 0L);

        assertFalse(limits.idleTimeoutEnabled());
    }

    @Test
    void rejectsNonPositiveConnectionOrHandshakeBudgets() {
        assertThrows(IllegalArgumentException.class, () -> new BinaryIngressLimits(0, 1_000L, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> new BinaryIngressLimits(8, 0L, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> new BinaryIngressLimits(8, 1_000L, -1L));
    }
}
