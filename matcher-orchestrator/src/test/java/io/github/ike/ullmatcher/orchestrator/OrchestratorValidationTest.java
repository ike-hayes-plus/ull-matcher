package io.github.ike.ullmatcher.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

final class OrchestratorValidationTest {
    @Test
    void rejectsInvalidRecordFields() {
        assertThrows(IllegalArgumentException.class, () -> new ShardEndpoints("", 8080, 9090, 10080));
        assertThrows(IllegalArgumentException.class, () -> new ShardEndpoints("127.0.0.1", 0, 9090, 10080));
        assertThrows(IllegalArgumentException.class, () -> new RegisteredShard(
                "",
                1,
                "node-a",
                new ShardEndpoints("127.0.0.1", 8080, 9090, 10080),
                ShardLifecycleState.ACTIVE,
                1L,
                1L
        ));
        assertThrows(IllegalArgumentException.class, () -> new SymbolRoute(
                0,
                "merchant:1",
                1L,
                1L,
                null
        ));
    }

    @Test
    void rejectsInvalidCodecPayloads() {
        assertThrows(IllegalStateException.class, () -> OrchestratorPayloadCodec.decodeShard("k", "bad"));
        assertThrows(IllegalStateException.class, () -> OrchestratorPayloadCodec.decodeSymbolRoute(1, "a|b"));
    }

    @Test
    void inMemoryStoreValidatesUnknownShard() throws Exception {
        try (InMemoryOrchestratorStore store = new InMemoryOrchestratorStore()) {
            assertThrows(java.io.IOException.class, () -> store.bindSymbol(1, "missing", 1L));
            assertThrows(java.io.IOException.class, () -> store.markDraining("missing"));
        }
    }
}
