package io.github.ike.ullmatcher.server.orchestrator;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.orchestrator.InMemoryOrchestratorStore;
import io.github.ike.ullmatcher.orchestrator.SymbolRoute;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class OrchestratorShardLifecycleTest {
    @TempDir
    Path dir;

    @Test
    void registersAndUnregistersOnClose() throws Exception {
        try (InMemoryOrchestratorStore store = new InMemoryOrchestratorStore()) {
            MatcherServerConfig config = serverConfig(dir, 9);
            try (OrchestratorShardLifecycle lifecycle = new OrchestratorShardLifecycle(
                    config, store, 2L, 60_000L)) {
                lifecycle.registerAdvertisedEndpoints("127.0.0.1", 8080, 9090, 10080);

                SymbolRoute route = store.lookupRoute(9).orElseThrow();
                assertTrue(route.activeShard().isPresent());
                assertEquals(2L, route.generation());
            }

            assertTrue(store.lookupRoute(9).isEmpty());
            assertTrue(store.getShard("merchant:9").isEmpty());
        }
    }

    private static MatcherServerConfig serverConfig(Path dir, int symbolId) {
        return MatcherServerConfig.builder("node-a", symbolId, dir)
                .shardKey("merchant:" + symbolId)
                .matcherConfig(MatcherConfig.defaults(symbolId))
                .build();
    }
}
