package io.github.ike.ullmatcher.sdk;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherOrchestratorClientTest {
    private static final JsonMapper MAPPER = JsonMapper.builderWithJackson2Defaults().build();

    @Test
    void resolvesSymbolRouteAndBuildsShardClientConfig() throws Exception {
        String body = """
                {
                  "symbolId": 7,
                  "shardKey": "merchant:7",
                  "generation": 2,
                  "updatedAtEpochMillis": 123,
                  "active": true,
                  "http": {"host": "127.0.0.1", "port": 18080},
                  "grpcPort": 19090,
                  "binaryIngressPort": 10080,
                  "nodeId": "node-a",
                  "state": "ACTIVE"
                }
                """;
        try (MiniHttpServer server = MiniHttpServer.start(exchange -> {
            assertEquals("/api/v1/orchestrator/routes/symbols/7", exchange.getRequestURI().getPath());
            respond(exchange, 200, body);
        })) {
            MatcherOrchestratorClient orchestrator = new MatcherOrchestratorClient(
                    new MatcherClientConfig(server.endpoint(), Duration.ofSeconds(2)));
            SymbolRouteView route = orchestrator.resolveSymbolRoute(7);
            assertEquals("merchant:7", route.shardKey());
            assertTrue(route.active());
            MatcherClientConfig shard = route.toMatcherClientConfig(Duration.ofSeconds(3), "secret");
            assertEquals(URI.create("http://127.0.0.1:18080"), shard.endpoint());
            assertEquals("secret", shard.ingressApiKey());
        }
    }

    @Test
    void notFoundMapsToMatcherClientException() throws Exception {
        try (MiniHttpServer server = MiniHttpServer.start(exchange ->
                respond(exchange, 404, "{\"error\":\"symbol route not found\"}"))) {
            MatcherOrchestratorClient orchestrator = new MatcherOrchestratorClient(
                    new MatcherClientConfig(server.endpoint(), Duration.ofSeconds(2)));
            MatcherClientException error = assertThrows(
                    MatcherClientException.class,
                    () -> orchestrator.resolveSymbolRoute(99));
            assertEquals(404, error.statusCode());
        }
    }

    @Test
    void parseRouteRequiresHttpBlock() throws Exception {
        assertThrows(MatcherClientException.class, () -> MatcherOrchestratorClient.parseRoute(
                MAPPER.readTree("{\"symbolId\":1,\"shardKey\":\"a\",\"generation\":0,\"updatedAtEpochMillis\":0}")));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    private record MiniHttpServer(HttpServer server) implements AutoCloseable {
        static MiniHttpServer start(Handler handler) throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> handler.handle(exchange));
            server.start();
            return new MiniHttpServer(server);
        }

        URI endpoint() {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
