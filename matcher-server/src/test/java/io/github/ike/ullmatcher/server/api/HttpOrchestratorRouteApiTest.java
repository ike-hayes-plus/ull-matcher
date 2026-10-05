package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.orchestrator.InMemoryOrchestratorStore;
import io.github.ike.ullmatcher.orchestrator.RegisteredShard;
import io.github.ike.ullmatcher.orchestrator.ShardEndpoints;
import io.github.ike.ullmatcher.orchestrator.ShardLifecycleState;
import io.github.ike.ullmatcher.orchestrator.SymbolRoute;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.orchestrator.OrchestratorRegistrationConfig;
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpOrchestratorRouteApiTest {
    @Test
    void returnsSymbolRouteWhenRegistered() throws Exception {
        InMemoryOrchestratorStore store = new InMemoryOrchestratorStore();
        RegisteredShard shard = new RegisteredShard(
                "merchant:7",
                7,
                "node-a",
                new ShardEndpoints("127.0.0.1", 18080, 19090, 10080),
                ShardLifecycleState.ACTIVE,
                2L,
                123L
        );
        store.registerShard(shard);
        store.bindSymbol(7, "merchant:7", 2L);

        Path dir = Files.createTempDirectory("http-orchestrator-route");
        try (MatcherNodeService nodeService = new MatcherNodeService(baseConfig(dir))) {
            nodeService.start();
            try (HttpApiServer server = orchestratorServer(nodeService, symbolId -> store.lookupRoute(symbolId))) {
                server.start();
                HttpResponse<String> response = HttpClient.newHttpClient().send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port()
                                + "/api/v1/orchestrator/routes/symbols/7")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
                assertTrue(response.body().contains("\"shardKey\":\"merchant:7\""));
                assertTrue(response.body().contains("\"active\":true"));
                assertTrue(response.body().contains("\"port\":18080"));
            }
        }
    }

    @Test
    void missingSymbolReturns404() throws Exception {
        Path dir = Files.createTempDirectory("http-orchestrator-missing");
        try (MatcherNodeService nodeService = new MatcherNodeService(baseConfig(dir))) {
            nodeService.start();
            try (HttpApiServer server = orchestratorServer(nodeService, symbolId -> Optional.<SymbolRoute>empty())) {
                server.start();
                HttpResponse<String> response = HttpClient.newHttpClient().send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port()
                                + "/api/v1/orchestrator/routes/symbols/99")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertEquals(404, response.statusCode());
            }
        }
    }

    private static MatcherServerConfig baseConfig(Path dir) {
        return new MatcherServerConfig(
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
                IngressAuthConfig.disabled(),
                0,
                GrpcReplicationServerConfig.defaults(0),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                OrchestratorRegistrationConfig.disabled(),
                null
        );
    }

    private static HttpApiServer orchestratorServer(
            MatcherNodeService nodeService,
            OrchestratorRouteLookup lookup) {
        return new HttpApiServer(
                0, "127.0.0.1", 2, 256, 256, 2_000L,
                128, 96, 16, 2_000L, 1_000L, 5_000L,
                96, 64, 2, 16, 8, "symbol-1", WriteAdmissionPolicyConfig.defaults(),
                HttpSubmitAckMode.LOCAL, MatcherServerMode.DEV, IngressAuthConfig.disabled(), nodeService,
                new GrpcTransportMetrics(),
                () -> new ClusterSupervisorMetricsSnapshot(
                        0L, 0L, null, null, Map.of(), List.of(), "IDLE", "", TransportMetricsSnapshot.none("NONE")),
                () -> new ReadinessSnapshot(
                        true, true, true, false, false, false, 0L, 0L, 0L, "", "READY", List.of(),
                        null, null, "NONE", "", "", 0L, 0L, 0L, 0L,
                        "STABLE", "transport policy is stable", "DISABLED",
                        "sequence reconciliation is disabled for this transport mode", "ready"
                ),
                BinaryOrderIngressServer.BinaryIngressConnectionMetrics::none,
                lookup
        );
    }
}
