package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpDispatchExecutorTest {
    @Test
    void defaultUsesVirtualThreadsWithoutPlatformQueue() {
        try (HttpDispatchExecutor executor = HttpDispatchExecutor.create(4, 256)) {
            assertFalse(executor.platformExecutorSaturationChecksEnabled());
            assertEquals(0, executor.executorQueueCapacity());
            assertEquals(0, executor.executorQueueDepth());
        }
    }

    @Test
    void platformPoolModeUsesBoundedQueueWhenPropertySet() {
        String previous = System.setProperty("matcher.httpPlatformReadExecutor", "true");
        try (HttpDispatchExecutor executor = HttpDispatchExecutor.create(4, 256)) {
            assertTrue(executor.platformExecutorSaturationChecksEnabled());
            assertEquals(4, executor.platformWorkerCount());
            assertTrue(executor.executorQueueCapacity() >= 4);
        } finally {
            if (previous == null) {
                System.clearProperty("matcher.httpPlatformReadExecutor");
            } else {
                System.setProperty("matcher.httpPlatformReadExecutor", previous);
            }
        }
    }
}
