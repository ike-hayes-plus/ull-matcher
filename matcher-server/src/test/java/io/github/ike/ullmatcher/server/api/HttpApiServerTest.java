package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.server.telemetry.ReadinessSnapshot;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpApiServerTest {
    @Test
    void serviceFailurePayloadHidesDetailInProdMode() {
        ServiceUnavailableException apiError = new ServiceUnavailableException(
                "submit order failed",
                new java.io.IOException("disk path /internal/wal failed")
        );

        Map<String, Object> prodPayload = HttpApiServer.serviceFailurePayload(
                MatcherServerMode.PROD,
                apiError,
                apiError.getCause()
        );
        Map<String, Object> devPayload = HttpApiServer.serviceFailurePayload(
                MatcherServerMode.DEV,
                apiError,
                apiError.getCause()
        );

        assertEquals("submit order failed", prodPayload.get("error"));
        assertEquals("service_unavailable", prodPayload.get("code"));
        assertFalse(prodPayload.containsKey("detail"));
        assertEquals("disk path /internal/wal failed", devPayload.get("detail"));
    }

    @Test
    void submissionEndpointProvidesIdempotentReceiptAndQuery() throws Exception {
        Path dir = Files.createTempDirectory("http-api-submission");
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "symbol-1",
                MatcherConfig.defaults(1),
                dir.resolve("wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                dir.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 10,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                0,
                "127.0.0.1",
                2,
                256,
                256,
                2_000L,
                128,
                96,
                16,
                2_000L,
                1_000L,
                5_000L,
                96,
                64,
                2,
                16,
                8,
                WriteAdmissionPolicyConfig.defaults(),
                false,
                0,
                GrpcReplicationServerConfig.defaults(0),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            try (HttpApiServer server = new HttpApiServer(
                    0, "127.0.0.1", 2, 256, 256, 2_000L,
                    128, 96, 16, 2_000L, 1_000L, 5_000L,
                    96, 64, 2, 16, 8, "symbol-1", WriteAdmissionPolicyConfig.defaults(),
                    nodeService, new GrpcTransportMetrics(),
                    () -> new ClusterSupervisorMetricsSnapshot(0L, 0L, null, null, Map.of(), List.of(), "IDLE", "", TransportMetricsSnapshot.none("NONE")),
                    () -> new ReadinessSnapshot(
                            true, true, true, false, false, false, 0L, 0L, 0L, "", "READY", List.of(),
                            null, null, "NONE", "", "", 0L, 0L, 0L, 0L,
                            "STABLE", "transport policy is stable", "DISABLED",
                            "sequence reconciliation is disabled for this transport mode", "ready"
                    ))) {
                server.start();
                HttpClient client = HttpClient.newHttpClient();
                String body = "{\"userId\":1,\"orderId\":1001,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":100,\"quantity\":1}";

                HttpResponse<String> first = client.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create("http://127.0.0.1:" + server.port() + "/api/v1/orders"))
                                .header("Content-Type", "application/json")
                                .header("Idempotency-Key", "submit-1001")
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .build(),
                        HttpResponse.BodyHandlers.ofString()
                );
                assertTrue(Set.of(200, 202).contains(first.statusCode()));
                @SuppressWarnings("unchecked")
                Map<String, Object> firstPayload = new com.fasterxml.jackson.databind.ObjectMapper().readValue(first.body(), Map.class);
                String submissionId = firstPayload.get("submissionId").toString();

                HttpResponse<String> second = client.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create("http://127.0.0.1:" + server.port() + "/api/v1/orders"))
                                .header("Content-Type", "application/json")
                                .header("Idempotency-Key", "submit-1001")
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .build(),
                        HttpResponse.BodyHandlers.ofString()
                );
                assertTrue(Set.of(200, 202).contains(second.statusCode()));
                @SuppressWarnings("unchecked")
                Map<String, Object> secondPayload = new com.fasterxml.jackson.databind.ObjectMapper().readValue(second.body(), Map.class);
                assertEquals(submissionId, secondPayload.get("submissionId"));

                String changedBody = "{\"userId\":1,\"orderId\":1001,\"side\":\"BUY\",\"orderType\":\"LIMIT\"," +
                        "\"timeInForce\":\"GTC\",\"price\":101,\"quantity\":1}";
                HttpResponse<String> changed = client.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create("http://127.0.0.1:" + server.port() + "/api/v1/orders"))
                                .header("Content-Type", "application/json")
                                .header("Idempotency-Key", "submit-1001")
                                .POST(HttpRequest.BodyPublishers.ofString(changedBody))
                                .build(),
                        HttpResponse.BodyHandlers.ofString()
                );
                assertEquals(409, changed.statusCode());
                assertTrue(changed.body().contains("idempotency key reused with different request"));

                HttpResponse<String> query = client.send(
                        request(server.port(), "GET", "/api/v1/submissions/" + submissionId, null),
                        HttpResponse.BodyHandlers.ofString()
                );
                assertEquals(200, query.statusCode());
                assertTrue(query.body().contains("\"replicationCommitted\":true"));
                assertTrue(query.body().contains("\"orderId\":1001"));
            }
        }
    }

    @Test
    void committedAckWaitsForReplicationCommit() throws Exception {
        Path dir = Files.createTempDirectory("http-api-committed-ack");
        MatcherServerConfig config = MatcherServerConfig.defaults("node-a", 1, dir);
        ControlledReplicator replicator = new ControlledReplicator();
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            nodeService.configureReplication(replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TimeUnit.SECONDS.toNanos(1));
            try (HttpApiServer server = new HttpApiServer(
                    0, "127.0.0.1", 2, 256, 256, 2_000L,
                    128, 96, 16, 2_000L, 1_000L, 5_000L,
                    96, 64, 2, 16, 8, "symbol-1", WriteAdmissionPolicyConfig.defaults(),
                    HttpSubmitAckMode.LOCAL,
                    nodeService, new GrpcTransportMetrics(),
                    () -> new ClusterSupervisorMetricsSnapshot(0L, 0L, null, null, Map.of(), List.of(), "IDLE", "", TransportMetricsSnapshot.none("NONE")),
                    () -> new ReadinessSnapshot(
                            true, true, true, false, false, false, 0L, 0L, 0L, "", "READY", List.of(),
                            null, null, "NONE", "", "", 0L, 0L, 0L, 0L,
                            "STABLE", "transport policy is stable", "DISABLED",
                            "sequence reconciliation is disabled for this transport mode", "ready"
                    ))) {
                server.start();
                HttpClient client = HttpClient.newHttpClient();
                String body = "{\"userId\":1,\"orderId\":2001,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":100,\"quantity\":1,\"ack\":\"committed\"}";

                CompletableFuture<HttpResponse<String>> response = client.sendAsync(
                        HttpRequest.newBuilder()
                                .uri(URI.create("http://127.0.0.1:" + server.port() + "/api/v1/orders"))
                                .header("Content-Type", "application/json")
                                .header("Idempotency-Key", "submit-2001")
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .build(),
                        HttpResponse.BodyHandlers.ofString()
                );

                assertTrue(replicator.awaitInvocations(1));
                Thread.sleep(50L);
                assertFalse(response.isDone());

                replicator.completeNext(new ReplicationResult(1, 1, List.of("standby-a"), List.of()));

                HttpResponse<String> committed = response.get(2, TimeUnit.SECONDS);
                assertEquals(200, committed.statusCode());
                assertTrue(committed.body().contains("\"phase\":\"COMMITTED\""));
                assertTrue(committed.body().contains("\"replicationCommitted\":true"));
            }
        }
    }

    @Test
    void batchSubmissionEndpointReturnsPerOrderReceipts() throws Exception {
        Path dir = Files.createTempDirectory("http-api-batch-submission");
        MatcherServerConfig config = MatcherServerConfig.defaults("node-a", 1, dir);
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            try (HttpApiServer server = new HttpApiServer(
                    0, "127.0.0.1", 2, 2048, 256, 2_000L,
                    128, 96, 16, 2_000L, 1_000L, 5_000L,
                    96, 64, 2, 16, 8, "symbol-1", WriteAdmissionPolicyConfig.defaults(),
                    nodeService, new GrpcTransportMetrics(),
                    () -> new ClusterSupervisorMetricsSnapshot(0L, 0L, null, null, Map.of(), List.of(), "IDLE", "", TransportMetricsSnapshot.none("NONE")),
                    () -> new ReadinessSnapshot(
                            true, true, true, false, false, false, 0L, 0L, 0L, "", "READY", List.of(),
                            null, null, "NONE", "", "", 0L, 0L, 0L, 0L,
                            "STABLE", "transport policy is stable", "DISABLED",
                            "sequence reconciliation is disabled for this transport mode", "ready"
                    ))) {
                server.start();
                HttpClient client = HttpClient.newHttpClient();
                String body = """
                        {
                          "ack":"local",
                          "orders":[
                            {"userId":1,"orderId":3001,"side":"BUY","orderType":"LIMIT","timeInForce":"GTC","price":100,"quantity":1,"idempotencyKey":"batch-3001"},
                            {"userId":1,"orderId":3002,"side":"BUY","orderType":"LIMIT","timeInForce":"GTC","price":101,"quantity":1,"idempotencyKey":"batch-3002"}
                          ]
                        }
                        """;

                HttpResponse<String> response = client.send(
                        request(server.port(), "POST", "/api/v1/orders/batch", body),
                        HttpResponse.BodyHandlers.ofString()
                );

                assertTrue(Set.of(200, 202).contains(response.statusCode()));
                @SuppressWarnings("unchecked")
                Map<String, Object> payload = new com.fasterxml.jackson.databind.ObjectMapper().readValue(response.body(), Map.class);
                assertEquals(2, payload.get("accepted"));
                assertEquals(0, payload.get("failed"));
                assertEquals(2, payload.get("count"));
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> submissions = (List<Map<String, Object>>) payload.get("submissions");
                assertEquals(2, submissions.size());
                assertEquals(3001, submissions.get(0).get("orderId"));
                assertEquals(3002, submissions.get(1).get("orderId"));
                assertTrue(submissions.get(0).containsKey("submissionId"));
                assertTrue(submissions.get(1).containsKey("submissionId"));
            }
        }
    }

    @Test
    void batchSubmissionRejectsOversizedBatch() throws Exception {
        Path dir = Files.createTempDirectory("http-api-batch-too-large");
        MatcherServerConfig config = MatcherServerConfig.defaults("node-a", 1, dir);
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            try (HttpApiServer server = new HttpApiServer(
                    0, "127.0.0.1", 2, 262_144, 256, 2_000L,
                    128, 96, 16, 2_000L, 1_000L, 5_000L,
                    96, 64, 2, 16, 8, "symbol-1", WriteAdmissionPolicyConfig.defaults(),
                    nodeService, new GrpcTransportMetrics(),
                    () -> new ClusterSupervisorMetricsSnapshot(0L, 0L, null, null, Map.of(), List.of(), "IDLE", "", TransportMetricsSnapshot.none("NONE")),
                    () -> new ReadinessSnapshot(
                            true, true, true, false, false, false, 0L, 0L, 0L, "", "READY", List.of(),
                            null, null, "NONE", "", "", 0L, 0L, 0L, 0L,
                            "STABLE", "transport policy is stable", "DISABLED",
                            "sequence reconciliation is disabled for this transport mode", "ready"
                    ))) {
                server.start();
                String order = "{\"userId\":1,\"orderId\":%d,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":100,\"quantity\":1}";
                StringBuilder body = new StringBuilder("{\"orders\":[");
                for (int i = 0; i < 1_025; i++) {
                    if (i > 0) {
                        body.append(',');
                    }
                    body.append(order.formatted(4_000 + i));
                }
                body.append("]}");

                HttpResponse<String> response = HttpClient.newHttpClient().send(
                        request(server.port(), "POST", "/api/v1/orders/batch", body.toString()),
                        HttpResponse.BodyHandlers.ofString()
                );

                assertEquals(400, response.statusCode());
                assertTrue(response.body().contains("exceeds max batch size"));
            }
        }
    }

    @Test
    void undertowRoutesReturnExpectedStatusCodes() throws Exception {
        Path dir = Files.createTempDirectory("http-api-server");
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "symbol-1",
                MatcherConfig.defaults(1),
                dir.resolve("wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                dir.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 10,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                0,
                "127.0.0.1",
                2,
                256,
                256,
                2_000L,
                128,
                96,
                16,
                2_000L,
                1_000L,
                5_000L,
                96,
                64,
                2,
                16,
                8,
                WriteAdmissionPolicyConfig.defaults(),
                false,
                0,
                GrpcReplicationServerConfig.defaults(0),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            assertTrue(await(() -> nodeService.health().acceptingClientCommands(), 5_000L));
            try (HttpApiServer server = new HttpApiServer(
                    0,
                    "127.0.0.1",
                    2,
                    256,
                    256,
                    2_000L,
                    128,
                    96,
                    16,
                    2_000L,
                    1_000L,
                    5_000L,
                    96,
                    64,
                    2,
                    16,
                    8,
                    "symbol-1",
                    WriteAdmissionPolicyConfig.defaults(),
                    nodeService,
                    new GrpcTransportMetrics(),
                    () -> new ClusterSupervisorMetricsSnapshot(
                    0L, 0L, null, null, Map.of(), List.of(), "IDLE", "", TransportMetricsSnapshot.none("NONE")),
                    () -> new ReadinessSnapshot(
                            true, true, true, false, false, false, 0L, 0L, 0L, "", "READY", List.of(),
                            null, null, "NONE", "", "", 0L, 0L, 0L, 0L,
                            "STABLE", "transport policy is stable", "DISABLED",
                            "sequence reconciliation is disabled for this transport mode", "ready"
                    )
            )) {
                server.start();
                HttpClient client = HttpClient.newHttpClient();

                HttpResponse<String> badOrder = client.send(request(server.port(), "POST", "/api/v1/orders",
                        "{\"userId\":1,\"orderId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":100,\"quantity\":1"),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(400, badOrder.statusCode());
                assertTrue(badOrder.body().contains("bad_request"));

                HttpResponse<String> tooLarge = client.send(request(server.port(), "POST", "/api/v1/orders",
                        "{\"padding\":\"" + "x".repeat(1024) + "\"}"), HttpResponse.BodyHandlers.ofString());
                assertEquals(400, tooLarge.statusCode());

                HttpResponse<String> notFound = client.send(request(server.port(), "GET", "/api/v1/orders/9999", null),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(404, notFound.statusCode());

                HttpResponse<String> health = client.send(request(server.port(), "GET", "/api/v1/runtime/health", null),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(200, health.statusCode());
                assertTrue(health.body().contains("\"submissionPendingCount\""));
                assertTrue(health.body().contains("\"tradeCount\""));
                assertTrue(health.body().contains("\"replicationQueueDepth\""));
                assertTrue(health.body().contains("\"standbyAckFlushCount\""));

                HttpResponse<String> metrics = client.send(request(server.port(), "GET", "/metrics", null),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(200, metrics.statusCode());
                assertTrue(metrics.body().contains("ull_matcher_live_orders"));
                assertTrue(metrics.body().contains("ull_matcher_trade_events_total"));
                assertTrue(metrics.body().contains("ull_matcher_submission_pending"));
                assertTrue(metrics.body().contains("ull_matcher_replication_queue_depth"));
                assertTrue(metrics.body().contains("ull_matcher_standby_ack_flush_total"));
                assertTrue(metrics.body().contains("ull_matcher_http_route_requests_total{route=\"read\"}"));
                assertTrue(metrics.body().contains("ull_matcher_http_route_saturation{route=\"write\"}"));
                assertTrue(metrics.body().contains("ull_matcher_http_endpoint_requests_total{endpoint=\"metrics\"}"));
                assertTrue(metrics.body().contains("ull_matcher_http_endpoint_duration_bucket{endpoint=\"runtime_health\",le=\"1000\"}"));
                assertTrue(metrics.body().contains("ull_matcher_http_shard_write_rate_limit_per_second{shard=\"symbol-1\"}"));
                assertTrue(metrics.body().contains("ull_matcher_http_tenant_write_default_weight"));
                assertTrue(metrics.body().contains("ull_matcher_ha_transport_policy_status{status=\"STABLE\"} 1"));
            }
        }
    }

    @Test
    void undertowRejectsOverloadedRequests() throws Exception {
        Path dir = Files.createTempDirectory("http-api-overload");
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "symbol-1",
                MatcherConfig.defaults(1),
                dir.resolve("wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                dir.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 10,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                0,
                "127.0.0.1",
                2,
                256,
                1,
                50L,
                1,
                1,
                1,
                50L,
                50L,
                50L,
                1,
                1,
                1,
                1,
                1,
                new WriteAdmissionPolicyConfig(1, 0, "X-Ull-Tenant-Key", 0.0d, 0, 0.0d, 0, 1, "", "X-Ull-Tenant-Priority"),
                false,
                0,
                GrpcReplicationServerConfig.defaults(0),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            assertTrue(await(() -> nodeService.health().acceptingClientCommands(), 5_000L));

            CountDownLatch firstRequestEntered = new CountDownLatch(1);
            CountDownLatch releaseFirstRequest = new CountDownLatch(1);
            try (HttpApiServer server = new HttpApiServer(
                    0,
                    "127.0.0.1",
                    2,
                    256,
                    1,
                    50L,
                    1,
                    1,
                    1,
                    50L,
                    50L,
                    50L,
                    1,
                    1,
                    1,
                    1,
                    1,
                    "symbol-1",
                    new WriteAdmissionPolicyConfig(1, 0, "X-Ull-Tenant-Key", 0.0d, 0, 0.0d, 0, 1, "", "X-Ull-Tenant-Priority"),
                    nodeService,
                    new GrpcTransportMetrics(),
                    () -> new ClusterSupervisorMetricsSnapshot(
                    0L, 0L, null, null, Map.of(), List.of(), "IDLE", "", TransportMetricsSnapshot.none("NONE")),
                    () -> {
                        firstRequestEntered.countDown();
                        try {
                            releaseFirstRequest.await(250L, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return new ReadinessSnapshot(
                                true, true, true, false, false, false, 0L, 0L, 0L, "", "READY", List.of(),
                                null, null, "NONE", "", "", 0L, 0L, 0L, 0L,
                                "STABLE", "transport policy is stable", "DISABLED",
                                "sequence reconciliation is disabled for this transport mode", "ready"
                        );
                    }
            )) {
                server.start();
                HttpClient client = HttpClient.newHttpClient();

                var first = client.sendAsync(
                        request(server.port(), "GET", "/api/v1/runtime/readiness", null),
                        HttpResponse.BodyHandlers.ofString()
                );
                assertTrue(firstRequestEntered.await(2, TimeUnit.SECONDS));

                HttpResponse<String> overloaded = client.send(
                        request(server.port(), "GET", "/api/v1/runtime/readiness", null),
                        HttpResponse.BodyHandlers.ofString()
                );
                assertEquals(503, overloaded.statusCode());
                assertTrue(overloaded.body().contains("overloaded"));

                releaseFirstRequest.countDown();
                HttpResponse<String> completed = first.get(5, TimeUnit.SECONDS);
                assertEquals(200, completed.statusCode());

                HttpResponse<String> metrics = client.send(
                        request(server.port(), "GET", "/metrics", null),
                        HttpResponse.BodyHandlers.ofString()
                );
                assertEquals(200, metrics.statusCode());
                assertTrue(metrics.body().contains("ull_matcher_http_route_overload_total{route=\"read\"} 1"));
                assertTrue(metrics.body().contains("ull_matcher_http_endpoint_overload_total{endpoint=\"runtime_readiness\"} 1"));
                assertTrue(metrics.body().contains("ull_matcher_http_endpoint_budget_saturation{endpoint=\"runtime_readiness\",route=\"read\"} "));
                assertTrue(metrics.body().contains("ull_matcher_http_shard_write_saturation{shard=\"symbol-1\"}"));
                assertTrue(metrics.body().contains("ull_matcher_http_shard_write_rate_limited_total{shard=\"symbol-1\"} 0"));
            }
        }
    }

    private static HttpRequest request(int port, String method, String path, String body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path));
        if (body == null) {
            return "GET".equals(method) ? builder.GET().build() : builder.method(method, HttpRequest.BodyPublishers.noBody()).build();
        }
        return builder.header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private static boolean await(Check check, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (check.ok()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    private static final class ControlledReplicator implements CommandReplicator {
        private final java.util.Queue<CompletableFuture<ReplicationResult>> futures = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public ReplicationResult replicate(Command command, long timeoutNanos) throws java.io.IOException {
            throw new UnsupportedOperationException("async only");
        }

        @Override
        public CompletableFuture<ReplicationResult> replicateBatchAsync(List<Command> commands,
                                                                        ReplicationMode mode,
                                                                        long timeoutNanos) {
            CompletableFuture<ReplicationResult> future = new CompletableFuture<>();
            futures.add(future);
            invocations.incrementAndGet();
            return future;
        }

        private boolean awaitInvocations(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadline) {
                if (invocations.get() >= expected) {
                    return true;
                }
                Thread.sleep(10L);
            }
            return invocations.get() >= expected;
        }

        private void completeNext(ReplicationResult result) {
            CompletableFuture<ReplicationResult> future = futures.poll();
            if (future == null) {
                throw new AssertionError("no replication future");
            }
            future.complete(result);
        }
    }
}
