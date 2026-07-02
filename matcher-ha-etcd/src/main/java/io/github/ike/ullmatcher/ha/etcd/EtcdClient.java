package io.github.ike.ullmatcher.ha.etcd;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

final class EtcdClient implements Closeable {
    private final List<URI> endpoints;
    private final AtomicInteger preferredEndpoint = new AtomicInteger();
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Duration timeout;

    EtcdClient(String endpoint, long timeoutMillis) {
        this.endpoints = parseEndpoints(endpoint);
        this.timeout = Duration.ofMillis(timeoutMillis);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(this.timeout)
                .build();
    }

    KeyValue get(String key) throws IOException {
        JsonNode response = post("/v3/kv/range", Map.of("key", encode(key)));
        JsonNode kvs = response.path("kvs");
        if (!kvs.isArray() || kvs.isEmpty()) {
            return null;
        }
        return toKeyValue(kvs.get(0));
    }

    List<KeyValue> rangeByPrefix(String prefix) throws IOException {
        JsonNode response = post("/v3/kv/range", Map.of(
                "key", encode(prefix),
                "range_end", encode(prefixEnd(prefix))
        ));
        JsonNode kvs = response.path("kvs");
        ArrayList<KeyValue> values = new ArrayList<>();
        if (!kvs.isArray()) {
            return values;
        }
        for (JsonNode kv : kvs) {
            values.add(toKeyValue(kv));
        }
        return values;
    }

    long grantLease(long ttlSeconds) throws IOException {
        JsonNode response = post("/v3/lease/grant", Map.of("TTL", Long.toString(ttlSeconds)));
        String id = response.path("ID").asText("");
        if (id.isBlank()) {
            throw new IOException("etcd lease grant response missing ID");
        }
        return Long.parseLong(id);
    }

    boolean txnCreate(String key, String value, long leaseId) throws IOException {
        JsonNode response = post("/v3/kv/txn", Map.of(
                "compare", List.of(Map.of(
                        "key", encode(key),
                        "target", "VERSION",
                        "result", "EQUAL",
                        "version", "0"
                )),
                "success", List.of(Map.of("request_put", putRequest(key, value, leaseId))),
                "failure", List.of()
        ));
        return response.path("succeeded").asBoolean(false);
    }

    boolean txnReplaceIfValue(String key, String expectedValue, String newValue, long leaseId) throws IOException {
        JsonNode response = post("/v3/kv/txn", Map.of(
                "compare", List.of(Map.of(
                        "key", encode(key),
                        "target", "VALUE",
                        "result", "EQUAL",
                        "value", encode(expectedValue)
                )),
                "success", List.of(Map.of("request_put", putRequest(key, newValue, leaseId))),
                "failure", List.of()
        ));
        return response.path("succeeded").asBoolean(false);
    }

    void put(String key, String value, long leaseId) throws IOException {
        post("/v3/kv/put", putRequest(key, value, leaseId));
    }

    void delete(String key) throws IOException {
        post("/v3/kv/deleterange", Map.of("key", encode(key)));
    }

    private JsonNode post(String path, Object payload) throws IOException {
        byte[] body = objectMapper.writeValueAsBytes(payload);
        IOException lastFailure = null;
        int start = Math.floorMod(preferredEndpoint.get(), endpoints.size());
        for (int attempt = 0; attempt < endpoints.size(); attempt++) {
            int index = (start + attempt) % endpoints.size();
            URI endpoint = endpoints.get(index);
            HttpRequest request = HttpRequest.newBuilder(endpoint.resolve(path))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    lastFailure = new IOException("etcd request failed endpoint=" + endpoint + " status=" + response.statusCode()
                            + " path=" + path + " body=" + response.body());
                    continue;
                }
                preferredEndpoint.set(index);
                return objectMapper.readTree(response.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while calling etcd path " + path, e);
            } catch (IOException e) {
                lastFailure = new IOException("etcd request failed endpoint=" + endpoint + " path=" + path, e);
            }
        }
        throw lastFailure == null ? new IOException("etcd request failed path=" + path) : lastFailure;
    }

    private static Map<String, Object> putRequest(String key, String value, long leaseId) {
        return Map.of(
                "key", encode(key),
                "value", encode(value),
                "lease", Long.toString(leaseId)
        );
    }

    private static List<URI> parseEndpoints(String rawEndpoints) {
        Objects.requireNonNull(rawEndpoints, "endpoint");
        ArrayList<URI> parsed = new ArrayList<>();
        for (String token : rawEndpoints.split(",")) {
            String endpoint = token.trim();
            if (!endpoint.isEmpty()) {
                parsed.add(URI.create(endpoint.endsWith("/") ? endpoint : endpoint + "/"));
            }
        }
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException("at least one etcd endpoint is required");
        }
        return List.copyOf(parsed);
    }

    private static KeyValue toKeyValue(JsonNode node) {
        return new KeyValue(decode(node.path("key").asText()), decode(node.path("value").asText()));
    }

    static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static String prefixEnd(String prefix) {
        byte[] bytes = prefix.getBytes(StandardCharsets.UTF_8);
        for (int i = bytes.length - 1; i >= 0; i--) {
            if ((bytes[i] & 0xff) != 0xff) {
                bytes[i]++;
                return new String(bytes, 0, i + 1, StandardCharsets.UTF_8);
            }
        }
        return "\0";
    }

    @Override
    public void close() {
    }

    record KeyValue(String key, String value) {
    }
}
