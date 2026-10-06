package io.github.ike.ullmatcher.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class InMemoryOrchestratorStoreTest {
    @Test
    void registerBindAndLookupRoute() throws Exception {
        try (InMemoryOrchestratorStore store = new InMemoryOrchestratorStore()) {
            RegisteredShard shard = sampleShard("merchant:42", 7, ShardLifecycleState.ACTIVE);
            store.registerShard(shard);
            store.bindSymbol(7, "merchant:42", 1L);

            SymbolRoute route = store.lookupRoute(7).orElseThrow();
            assertEquals(7, route.symbolId());
            assertEquals("merchant:42", route.shardKey());
            assertTrue(route.activeShard().isPresent());
            assertEquals("node-a", route.activeShard().orElseThrow().nodeId());
        }
    }

    @Test
    void lookupReturnsEmptyWhenShardIsGone() throws Exception {
        try (InMemoryOrchestratorStore store = new InMemoryOrchestratorStore()) {
            store.registerShard(sampleShard("merchant:42", 7, ShardLifecycleState.ACTIVE));
            store.bindSymbol(7, "merchant:42", 1L);
            store.unregisterShard("merchant:42");

            assertTrue(store.lookupRoute(7).isEmpty());
        }
    }

    @Test
    void drainingShardIsExcludedFromActiveRoute() throws Exception {
        try (InMemoryOrchestratorStore store = new InMemoryOrchestratorStore()) {
            store.registerShard(sampleShard("merchant:42", 7, ShardLifecycleState.ACTIVE));
            store.bindSymbol(7, "merchant:42", 1L);
            store.markDraining("merchant:42");

            SymbolRoute route = store.lookupRoute(7).orElseThrow();
            assertFalse(route.activeShard().isPresent());
            assertEquals(ShardLifecycleState.DRAINING, route.shard().state());
        }
    }

    @Test
    void routingTableRefreshBuildsCache() throws Exception {
        try (InMemoryOrchestratorStore store = new InMemoryOrchestratorStore()) {
            store.registerShard(sampleShard("merchant:42", 7, ShardLifecycleState.ACTIVE));
            store.bindSymbol(7, "merchant:42", 2L);

            RoutingTable table = new RoutingTable(store);
            table.refresh();

            SymbolRoute route = table.route(7).orElseThrow();
            assertEquals(2L, route.generation());
            assertEquals(1, table.routesSnapshot().size());
        }
    }

    @Test
    void routingTableExposesShardSnapshot() throws Exception {
        try (InMemoryOrchestratorStore store = new InMemoryOrchestratorStore()) {
            store.registerShard(sampleShard("merchant:42", 7, ShardLifecycleState.ACTIVE));
            RoutingTable table = new RoutingTable(store);
            table.refresh();
            assertTrue(table.shard("merchant:42").isPresent());
        }
    }

    @Test
    void codecRoundTripSymbolRoutePayload() {
        String encoded = OrchestratorPayloadCodec.encodeSymbolRoute(9, "merchant:9", 4L, 99L);
        OrchestratorPayloadCodec.SymbolRouteHeader header = OrchestratorPayloadCodec.decodeSymbolRoute(9, encoded);
        assertEquals("merchant:9", header.shardKey());
        assertEquals(4L, header.generation());
    }

    @Test
    void codecRoundTripShardPayload() {
        RegisteredShard shard = sampleShard("merchant:99", 3, ShardLifecycleState.ACTIVE);
        RegisteredShard decoded = OrchestratorPayloadCodec.decodeShard(
                shard.shardKey(),
                OrchestratorPayloadCodec.encodeShard(shard)
        );
        assertEquals(shard, decoded);
    }

    private static RegisteredShard sampleShard(String shardKey, int symbolId, ShardLifecycleState state) {
        return new RegisteredShard(
                shardKey,
                symbolId,
                "node-a",
                new ShardEndpoints("10.0.0.1", 8080, 9090, 10080),
                state,
                1L,
                1_700_000_000_000L
        );
    }
}
