package io.github.ike.ullmatcher.ha.etcd;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.orchestrator.RegisteredShard;
import io.github.ike.ullmatcher.orchestrator.ShardEndpoints;
import io.github.ike.ullmatcher.orchestrator.ShardLifecycleState;
import io.github.ike.ullmatcher.orchestrator.SymbolRoute;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void isHeldBySeesLeaseLossWithoutLocalCacheWindow() throws Exception {
        try (FakeEtcdServer server = new FakeEtcdServer();
             EtcdLeaseStore store = new EtcdLeaseStore(config(server.endpoint()))) {
            long nowNanos = System.nanoTime();
            assertTrue(store.tryAcquire("node-a", new FencingToken(1L), nowNanos, TTL_NANOS));
            assertTrue(store.isHeldBy("node-a", new FencingToken(1L), nowNanos + 1L));

            server.deleteKey("/ull-matcher/test-etcd/lease/primary");

            assertNull(store.currentLease());
            assertFalse(store.isHeldBy("node-a", new FencingToken(1L), nowNanos + 2L),
                    "lost etcd lease must fail isHeldBy immediately; a positive local cache would still return true");
        }
    }

    @Test
    void orchestratorStoreRegistersShardBindsSymbolAndResolvesRoute() throws Exception {
        try (FakeEtcdServer server = new FakeEtcdServer();
             EtcdOrchestratorStore store = new EtcdOrchestratorStore(config(server.endpoint()))) {
            RegisteredShard shard = new RegisteredShard(
                    "merchant:42",
                    7,
                    "node-a",
                    new ShardEndpoints("127.0.0.1", 8080, 9090, 10080),
                    ShardLifecycleState.ACTIVE,
                    1L,
                    System.currentTimeMillis()
            );
            store.registerShard(shard);
            store.bindSymbol(7, "merchant:42", 3L);

            SymbolRoute route = store.lookupRoute(7).orElseThrow();
            assertEquals("merchant:42", route.shardKey());
            assertEquals(3L, route.generation());
            assertTrue(route.activeShard().isPresent());

            store.markDraining("merchant:42");
            SymbolRoute drained = store.lookupRoute(7).orElseThrow();
            assertFalse(drained.activeShard().isPresent());

            assertEquals(1, store.listShards().size());
            store.unregisterShard("merchant:42");
            assertTrue(store.listShards().isEmpty());
            assertTrue(store.lookupRoute(7).isEmpty());
            assertTrue(store.getShard("merchant:42").isEmpty());
        }
    }

    @Test
    void orchestratorStoreRejectsSymbolBindForMissingShard() throws Exception {
        try (FakeEtcdServer server = new FakeEtcdServer();
             EtcdOrchestratorStore store = new EtcdOrchestratorStore(config(server.endpoint()))) {
            assertThrows(IOException.class, () -> store.bindSymbol(1, "missing", 1L));
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

    @Test
    void leaseStoreTalksToEtcdOverTlsUsingTheConfiguredTrustChain(@TempDir Path directory) throws Exception {
        TestPkiFixture pki = TestPkiFixture.create(directory, "RSA");
        try (FakeEtcdServer server = new FakeEtcdServer(pki);
             EtcdLeaseStore store = new EtcdLeaseStore(tlsConfig(server.endpoint(), pki))) {
            long nowNanos = System.nanoTime();

            assertTrue(store.tryAcquire("node-a", new FencingToken(7L), nowNanos, TTL_NANOS));
            assertTrue(store.isHeldBy("node-a", new FencingToken(7L), nowNanos + 1L));
            assertFalse(store.tryAcquire("node-b", new FencingToken(8L), nowNanos + 2L, TTL_NANOS));
        }
    }

    @Test
    void tlsHandshakeFailsWhenTheServerCertificateIsNotTrusted(@TempDir Path directory) throws Exception {
        TestPkiFixture serverPki = TestPkiFixture.create(directory.resolve("server"), "RSA");
        TestPkiFixture otherPki = TestPkiFixture.create(directory.resolve("other"), "RSA");
        try (FakeEtcdServer server = new FakeEtcdServer(serverPki);
             EtcdLeaseStore store = new EtcdLeaseStore(tlsConfig(server.endpoint(), otherPki))) {
            // The untrusted chain must stop the request; the store surfaces it as an unchecked failure.
            assertThrows(IllegalStateException.class, store::currentLease);
        }
    }

    private static EtcdConfig tlsConfig(String endpoint, TestPkiFixture pki) {
        return new EtcdConfig(endpoint, "/ull-matcher/test-etcd", 10L, 2_000L,
                pki.certificatePem(), null, null, false);
    }

    private static EtcdConfig config(String endpoint) {
        return new EtcdConfig(endpoint, "/ull-matcher/test-etcd", 10L, 2_000L, null, null, null, false);
    }

    private static final class FakeEtcdServer implements AutoCloseable {
        private final JsonMapper objectMapper = JsonMapper.builderWithJackson2Defaults().build();
        private final Map<String, String> values = new ConcurrentHashMap<>();
        private final AtomicLong nextLeaseId = new AtomicLong(100L);
        private final HttpServer server;
        private final String scheme;

        private FakeEtcdServer() throws IOException {
            this(null);
        }

        private FakeEtcdServer(TestPkiFixture pki) throws IOException {
            if (pki == null) {
                server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                scheme = "http";
            } else {
                server = httpsServer(pki);
                scheme = "https";
            }
            server.createContext("/v3/lease/grant", this::grantLease);
            server.createContext("/v3/kv/txn", this::txn);
            server.createContext("/v3/kv/put", this::put);
            server.createContext("/v3/kv/range", this::range);
            server.createContext("/v3/kv/deleterange", this::deleteRange);
            server.start();
        }

        private String endpoint() {
            return scheme + "://127.0.0.1:" + server.getAddress().getPort();
        }

        private static HttpsServer httpsServer(TestPkiFixture pki) throws IOException {
            try {
                KeyStore keyStore = KeyStore.getInstance("PKCS12");
                try (java.io.InputStream input = Files.newInputStream(pki.keyStoreFile())) {
                    keyStore.load(input, pki.keyStorePassword());
                }
                KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagers.init(keyStore, pki.keyStorePassword());
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagers.getKeyManagers(), null, null);
                HttpsServer https = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                https.setHttpsConfigurator(new HttpsConfigurator(sslContext));
                return https;
            } catch (GeneralSecurityException e) {
                throw new IOException("failed to start TLS fake etcd", e);
            }
        }

        private void grantLease(HttpExchange exchange) throws IOException {
            writeJson(exchange, Map.of("ID", Long.toString(nextLeaseId.incrementAndGet())));
        }

        private void txn(HttpExchange exchange) throws IOException {
            JsonNode request = readJson(exchange);
            boolean matched = true;
            for (JsonNode compare : request.path("compare")) {
                String key = EtcdClient.decode(compare.path("key").asString());
                String target = compare.path("target").asString();
                if ("VERSION".equals(target)) {
                    long expectedVersion = compare.path("version").asLong();
                    long actualVersion = values.containsKey(key) ? 1L : 0L;
                    matched &= actualVersion == expectedVersion;
                } else if ("VALUE".equals(target)) {
                    String expected = EtcdClient.decode(compare.path("value").asString());
                    matched &= expected.equals(values.get(key));
                } else {
                    matched = false;
                }
            }
            if (matched) {
                for (JsonNode success : request.path("success")) {
                    JsonNode put = success.path("request_put");
                    values.put(EtcdClient.decode(put.path("key").asString()), EtcdClient.decode(put.path("value").asString()));
                }
            }
            writeJson(exchange, Map.of("succeeded", matched));
        }

        private void put(HttpExchange exchange) throws IOException {
            JsonNode request = readJson(exchange);
            values.put(EtcdClient.decode(request.path("key").asString()), EtcdClient.decode(request.path("value").asString()));
            writeJson(exchange, Map.of());
        }

        private void range(HttpExchange exchange) throws IOException {
            JsonNode request = readJson(exchange);
            String key = EtcdClient.decode(request.path("key").asString());
            JsonNode rangeEndNode = request.path("range_end");
            List<Map<String, String>> kvs;
            if (rangeEndNode.isMissingNode()) {
                String value = values.get(key);
                kvs = value == null ? List.of() : List.of(kv(key, value));
            } else {
                String rangeEnd = EtcdClient.decode(rangeEndNode.asString());
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
            values.remove(EtcdClient.decode(request.path("key").asString()));
            writeJson(exchange, Map.of());
        }

        private void deleteKey(String key) {
            values.remove(key);
        }

        private JsonNode readJson(HttpExchange exchange) throws IOException {
            try {
                return objectMapper.readTree(exchange.getRequestBody());
            } catch (JacksonException e) {
                throw new IOException("failed to read etcd test request", e);
            }
        }

        private void writeJson(HttpExchange exchange, Object body) throws IOException {
            final byte[] bytes;
            try {
                bytes = objectMapper.writeValueAsBytes(body);
            } catch (JacksonException e) {
                throw new IOException("failed to write etcd test response", e);
            }
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
