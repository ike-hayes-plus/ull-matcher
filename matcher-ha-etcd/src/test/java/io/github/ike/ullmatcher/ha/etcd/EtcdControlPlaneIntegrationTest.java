package io.github.ike.ullmatcher.ha.etcd;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EtcdControlPlaneIntegrationTest {
    private static final long TTL_NANOS = TimeUnit.SECONDS.toNanos(10);

    @Test
    void leaseStoreAcquiresExtendsAndRejectsCompetingOwnerOverHttpProtocol() throws Exception {
        try (FakeEtcdServer server = new FakeEtcdServer();
             EtcdLeaseStore store = new EtcdLeaseStore(config(server.endpoint()))) {
            long nowNanos = System.nanoTime();

            assertNull(store.currentLease());
            assertTrue(store.tryAcquire("node-a", new FencingToken(1L), nowNanos, TTL_NANOS));
            assertTrue(store.isHeldBy("node-a", new FencingToken(1L), nowNanos + 1L));

            ClusterLease lease = store.currentLease();
            assertNotNull(lease);
            assertEquals("node-a", lease.ownerNodeId());
            assertEquals(new FencingToken(1L), lease.fencingToken());

            assertFalse(store.tryAcquire("node-b", new FencingToken(2L), nowNanos + 2L, TTL_NANOS));
            assertTrue(store.tryExtend("node-a", new FencingToken(1L), nowNanos + 3L, TTL_NANOS));
            assertFalse(store.tryExtend("node-a", new FencingToken(2L), nowNanos + 4L, TTL_NANOS));
            assertFalse(store.tryExtend("node-b", new FencingToken(1L), nowNanos + 5L, TTL_NANOS));
        }
    }

    @Test
    void nodeRegistryRegistersUpdatesListsAndUnregistersOverHttpProtocol() throws Exception {
        try (FakeEtcdServer server = new FakeEtcdServer();
             EtcdNodeRegistry registry = new EtcdNodeRegistry(config(server.endpoint()))) {
            DiscoveredNode initial = new DiscoveredNode(
                    "node-a",
                    "10.0.0.11",
                    9090,
                    HaRole.STANDBY,
                    Map.of("shardKey", "symbol-1", "zone", "az-a")
            );
            registry.registerOrUpdate(initial);

            List<DiscoveredNode> listed = registry.listNodes();
            assertEquals(1, listed.size());
            assertEquals(initial, listed.get(0));

            DiscoveredNode updated = new DiscoveredNode(
                    "node-a",
                    "10.0.0.12",
                    9091,
                    HaRole.PRIMARY,
                    Map.of("shardKey", "symbol-1", "zone", "az-b")
            );
            registry.registerOrUpdate(updated);

            listed = registry.listNodes();
            assertEquals(1, listed.size());
            assertEquals(updated, listed.get(0));

            registry.unregister("node-a");
            assertTrue(registry.listNodes().isEmpty());
        }
    }

    private static EtcdConfig config(String endpoint) {
        return new EtcdConfig(endpoint, "/ull-matcher/test-etcd", 10L, 2_000L, 25L);
    }

    private static final class FakeEtcdServer implements AutoCloseable {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Map<String, String> values = new ConcurrentHashMap<>();
        private final AtomicLong nextLeaseId = new AtomicLong(100L);
        private final HttpServer server;

        private FakeEtcdServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v3/lease/grant", this::grantLease);
            server.createContext("/v3/kv/txn", this::txn);
            server.createContext("/v3/kv/put", this::put);
            server.createContext("/v3/kv/range", this::range);
            server.createContext("/v3/kv/deleterange", this::deleteRange);
            server.start();
        }

        private String endpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private void grantLease(HttpExchange exchange) throws IOException {
            writeJson(exchange, Map.of("ID", Long.toString(nextLeaseId.incrementAndGet())));
        }

        private void txn(HttpExchange exchange) throws IOException {
            JsonNode request = readJson(exchange);
            boolean matched = true;
            for (JsonNode compare : request.path("compare")) {
                String key = EtcdClient.decode(compare.path("key").asText());
                String target = compare.path("target").asText();
                if ("VERSION".equals(target)) {
                    long expectedVersion = compare.path("version").asLong();
                    long actualVersion = values.containsKey(key) ? 1L : 0L;
                    matched &= actualVersion == expectedVersion;
                } else if ("VALUE".equals(target)) {
                    String expected = EtcdClient.decode(compare.path("value").asText());
                    matched &= expected.equals(values.get(key));
                } else {
                    matched = false;
                }
            }
            if (matched) {
                for (JsonNode success : request.path("success")) {
                    JsonNode put = success.path("request_put");
                    values.put(EtcdClient.decode(put.path("key").asText()), EtcdClient.decode(put.path("value").asText()));
                }
            }
            writeJson(exchange, Map.of("succeeded", matched));
        }

        private void put(HttpExchange exchange) throws IOException {
            JsonNode request = readJson(exchange);
            values.put(EtcdClient.decode(request.path("key").asText()), EtcdClient.decode(request.path("value").asText()));
            writeJson(exchange, Map.of());
        }

        private void range(HttpExchange exchange) throws IOException {
            JsonNode request = readJson(exchange);
            String key = EtcdClient.decode(request.path("key").asText());
            JsonNode rangeEndNode = request.path("range_end");
            List<Map<String, String>> kvs;
            if (rangeEndNode.isMissingNode()) {
                String value = values.get(key);
                kvs = value == null ? List.of() : List.of(kv(key, value));
            } else {
                String rangeEnd = EtcdClient.decode(rangeEndNode.asText());
                kvs = values.entrySet().stream()
                        .filter(entry -> entry.getKey().compareTo(key) >= 0 && entry.getKey().compareTo(rangeEnd) < 0)
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(entry -> kv(entry.getKey(), entry.getValue()))
                        .toList();
            }
            writeJson(exchange, Map.of("kvs", kvs));
        }

        private void deleteRange(HttpExchange exchange) throws IOException {
            JsonNode request = readJson(exchange);
            values.remove(EtcdClient.decode(request.path("key").asText()));
            writeJson(exchange, Map.of());
        }

        private JsonNode readJson(HttpExchange exchange) throws IOException {
            return objectMapper.readTree(exchange.getRequestBody());
        }

        private void writeJson(HttpExchange exchange, Object body) throws IOException {
            byte[] bytes = objectMapper.writeValueAsBytes(body);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }

        private static Map<String, String> kv(String key, String value) {
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("key", EtcdClient.encode(key));
            entry.put("value", EtcdClient.encode(value));
            return entry;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
