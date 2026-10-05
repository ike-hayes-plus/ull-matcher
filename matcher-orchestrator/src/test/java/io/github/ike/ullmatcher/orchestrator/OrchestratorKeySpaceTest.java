package io.github.ike.ullmatcher.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class OrchestratorKeySpaceTest {
    @Test
    void buildsV3KeysFromDeploymentPrefix() {
        String root = OrchestratorKeySpace.v3Root("/ull-matcher/prod");
        assertEquals("/ull-matcher/prod/v3", root);
        assertEquals("/ull-matcher/prod/v3/shards/merchant:42", OrchestratorKeySpace.shardRecordKey(root, "merchant:42"));
        assertEquals("/ull-matcher/prod/v3/routes/symbols/7", OrchestratorKeySpace.symbolRouteKey(root, 7));
        assertEquals("/ull-matcher/prod/v3/shards/", OrchestratorKeySpace.shardPrefix(root));
    }

    @Test
    void rejectsInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> OrchestratorKeySpace.v3Root(""));
        assertThrows(IllegalArgumentException.class, () -> OrchestratorKeySpace.symbolRouteKey("/x/v3", 0));
    }

    @Test
    void normalizesTrailingSlashOnPrefix() {
        assertEquals("/ull/v3", OrchestratorKeySpace.v3Root("/ull/"));
    }
}
