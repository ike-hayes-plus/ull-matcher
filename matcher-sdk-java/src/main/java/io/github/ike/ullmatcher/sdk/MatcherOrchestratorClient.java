package io.github.ike.ullmatcher.sdk;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 只读编排 API 客户端：解析 {@code symbolId} → 分片 HTTP 端点。
 * <p>
 * 通常指向任意已开启 {@code matcher.orchestratorEnabled} 的 matcher-server 节点。
 */
public final class MatcherOrchestratorClient {
    private static final JsonMapper OBJECT_MAPPER = JsonMapper.builderWithJackson2Defaults().build();

    private final MatcherClientConfig config;
    private final HttpClient client;

    public MatcherOrchestratorClient(MatcherClientConfig config) {
        this(config, HttpClient.newBuilder()
                .connectTimeout(config.requestTimeout())
                .build());
    }

    public MatcherOrchestratorClient(MatcherClientConfig config, HttpClient client) {
        this.config = Objects.requireNonNull(config, "config");
        this.client = Objects.requireNonNull(client, "client");
    }

    public SymbolRouteView resolveSymbolRoute(int symbolId) {
        if (symbolId <= 0) {
            throw new IllegalArgumentException("symbolId must be positive");
        }
        HttpRequest request = baseRequest("/api/v1/orchestrator/routes/symbols/" + symbolId)
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() == 404) {
            throw new MatcherClientException("symbol route not found", response.statusCode(), response.body());
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new MatcherClientException("orchestrator route request failed", response.statusCode(), response.body());
        }
        try {
            return parseRoute(OBJECT_MAPPER.readTree(response.body()));
        } catch (JacksonException e) {
            throw new MatcherClientException("failed to parse orchestrator route response", e);
        }
    }

    static SymbolRouteView parseRoute(JsonNode root) {
        Objects.requireNonNull(root, "root");
        JsonNode http = root.get("http");
        if (http == null || http.isNull()) {
            throw new MatcherClientException("orchestrator route missing http endpoints", 200, root.toString());
        }
        Integer binaryIngressPort = null;
        JsonNode binaryNode = root.get("binaryIngressPort");
        if (binaryNode != null && !binaryNode.isNull()) {
            binaryIngressPort = binaryNode.intValue();
        }
        return new SymbolRouteView(
                root.get("symbolId").intValue(),
                text(root, "shardKey"),
                longField(root, "generation"),
                longField(root, "updatedAtEpochMillis"),
                root.path("active").booleanValue(),
                text(http, "host"),
                http.get("port").intValue(),
                root.path("grpcPort").intValue(),
                binaryIngressPort,
                optionalText(root, "nodeId"),
                optionalText(root, "state")
        );
    }

    private HttpRequest.Builder baseRequest(String pathAndQuery) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(resolve(pathAndQuery))
                .timeout(config.requestTimeout());
        if (config.ingressApiKey() != null) {
            builder.header(config.ingressApiKeyHeader(), config.ingressApiKey());
        }
        return builder;
    }

    private URI resolve(String pathAndQuery) {
        String base = config.endpoint().toString();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return URI.create(base + pathAndQuery);
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new MatcherClientException("orchestrator route request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MatcherClientException("orchestrator route request interrupted", e);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new MatcherClientException("orchestrator route missing field: " + field, 200, node.toString());
        }
        return value.asString();
    }

    private static String optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        return value.asString();
    }

    private static long longField(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new MatcherClientException("orchestrator route missing field: " + field, 200, node.toString());
        }
        return value.longValue();
    }
}
