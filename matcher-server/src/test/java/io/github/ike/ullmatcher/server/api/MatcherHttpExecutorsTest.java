package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

final class MatcherHttpExecutorsTest {
    @Test
    void sharedVirtualDispatchIsSingletonAcrossServerInstances() {
        assertEquals(0, MatcherHttpExecutors.httpServerInstancesForTests());
        try (HttpDispatchExecutor first = HttpDispatchExecutor.create(4, 256);
             HttpDispatchExecutor second = HttpDispatchExecutor.create(4, 256)) {
            assertEquals(2, MatcherHttpExecutors.httpServerInstancesForTests());
            assertSame(first.executor(), second.executor());
        }
        assertEquals(0, MatcherHttpExecutors.httpServerInstancesForTests());
    }
}
