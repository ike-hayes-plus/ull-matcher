package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.orchestrator.RegisteredShard;
import io.github.ike.ullmatcher.orchestrator.ShardEndpoints;
import io.github.ike.ullmatcher.orchestrator.SymbolRoute;

import java.util.LinkedHashMap;
import java.util.Map;

final class HttpOrchestratorRouteDocument {
    private HttpOrchestratorRouteDocument() {
    }

    static Map<String, Object> toResponseBody(SymbolRoute route) {
        RegisteredShard shard = route.shard();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("symbolId", route.symbolId());
        body.put("shardKey", route.shardKey());
        body.put("generation", route.generation());
        body.put("updatedAtEpochMillis", route.updatedAtEpochMillis());
        body.put("active", route.activeShard().isPresent());
        if (shard != null) {
            body.put("nodeId", shard.nodeId());
            body.put("state", shard.state().name());
            ShardEndpoints endpoints = shard.endpoints();
            body.put("http", Map.of("host", endpoints.httpHost(), "port", endpoints.httpPort()));
            body.put("grpcPort", endpoints.grpcPort());
            if (endpoints.binaryIngressPort() > 0) {
                body.put("binaryIngressPort", endpoints.binaryIngressPort());
            }
        }
        return body;
    }
}
