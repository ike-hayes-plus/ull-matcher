package io.github.ike.ullmatcher.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class MatcherHttpClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final MatcherClientConfig config;
    private final HttpClient client;

    public MatcherHttpClient(MatcherClientConfig config) {
        this(config, HttpClient.newBuilder()
                .connectTimeout(config.requestTimeout())
                .build());
    }

    public MatcherHttpClient(MatcherClientConfig config, HttpClient client) {
        this.config = Objects.requireNonNull(config, "config");
        this.client = Objects.requireNonNull(client, "client");
    }

    public JsonNode submitOrder(NewOrderRequest request) {
        return jsonRequest("POST", "/api/v1/orders", newOrderPayload(request).toString());
    }

    public JsonNode submitOrders(List<NewOrderRequest> requests) {
        Objects.requireNonNull(requests, "requests");
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("requests must not be empty");
        }
        ObjectNode payload = OBJECT_MAPPER.createObjectNode();
        ArrayNode orders = payload.putArray("orders");
        for (NewOrderRequest request : requests) {
            orders.add(newOrderPayload(request));
        }
        return jsonRequest("POST", "/api/v1/orders/batch", payload.toString());
    }

    private ObjectNode newOrderPayload(NewOrderRequest request) {
        Objects.requireNonNull(request, "request");
        ObjectNode payload = OBJECT_MAPPER.createObjectNode()
                .put("userId", request.userId())
                .put("orderId", request.orderId())
                .put("side", request.side())
                .put("orderType", request.orderType())
                .put("timeInForce", request.timeInForce())
                .put("price", request.price())
                .put("quantity", request.quantity());
        if (request.ttlMillis() != null) {
            payload.put("ttlMillis", request.ttlMillis());
        }
        if (request.idempotencyKey() != null && !request.idempotencyKey().isBlank()) {
            payload.put("idempotencyKey", request.idempotencyKey());
        }
        return payload;
    }

    public JsonNode cancelOrder(CancelOrderRequest request) {
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("orderId", request.orderId());
        if (request.idempotencyKey() != null && !request.idempotencyKey().isBlank()) {
            payload.put("idempotencyKey", request.idempotencyKey());
        }
        return jsonRequest("POST", "/api/v1/orders/cancel", payload.toString());
    }

    public JsonNode getOrder(long orderId) {
        return jsonRequest("GET", "/api/v1/orders/" + orderId, null);
    }

    public JsonNode recentOrders(int limit) {
        return jsonRequest("GET", "/api/v1/orders?limit=" + Math.max(1, limit), null);
    }

    public JsonNode getSubmission(String submissionId) {
        if (submissionId == null || submissionId.isBlank()) {
            throw new IllegalArgumentException("submissionId must not be blank");
        }
        return jsonRequest("GET", "/api/v1/submissions/" + encodePathSegment(submissionId), null);
    }

    public JsonNode getSubmissionByIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        return jsonRequest("GET", "/api/v1/submissions/by-idempotency?idempotencyKey=" + encodeQuery(idempotencyKey), null);
    }

    public JsonNode health() {
        return jsonRequest("GET", "/api/v1/runtime/health", null);
    }

    public JsonNode readiness() {
        return jsonRequest("GET", "/api/v1/runtime/readiness", null);
    }

    public JsonNode createSnapshot() {
        return jsonRequest("POST", "/api/v1/admin/snapshot", "{}");
    }

    public String metrics() {
        HttpRequest request = baseRequest("/metrics").GET().build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new MatcherClientException("matcher metrics request failed", response.statusCode(), response.body());
        }
        return response.body();
    }

    private JsonNode jsonRequest(String method, String pathAndQuery, String body) {
        HttpRequest.Builder builder = baseRequest(pathAndQuery)
                .header("Accept", "application/json");
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        HttpResponse<String> response = send(builder.build());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new MatcherClientException("matcher request failed", response.statusCode(), response.body());
        }
        try {
            return OBJECT_MAPPER.readTree(response.body());
        } catch (IOException e) {
            throw new MatcherClientException("failed to parse matcher response", e);
        }
    }

    private HttpRequest.Builder baseRequest(String pathAndQuery) {
        return HttpRequest.newBuilder(resolve(pathAndQuery))
                .timeout(config.requestTimeout());
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
            throw new MatcherClientException("matcher request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MatcherClientException("matcher request interrupted", e);
        }
    }

    private static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String encodeQuery(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    public Duration requestTimeout() {
        return config.requestTimeout();
    }
}
