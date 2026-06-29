package io.github.ike.ullmatcher.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherHttpClientTest {
    @Test
    void submitOrderPostsExpectedPayload() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().toString());
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 202, "{\"submissionId\":\"s1\",\"replicationCommitted\":false}");
        })) {
            MatcherHttpClient client = client(server);

            JsonNode response = client.submitOrder(NewOrderRequest.limit(7L, 101L, "BUY", "GTC", 12L, 3L, "k1"));

            assertEquals("POST", method.get());
            assertEquals("/api/v1/orders", path.get());
            assertTrue(body.get().contains("\"userId\":7"));
            assertTrue(body.get().contains("\"idempotencyKey\":\"k1\""));
            assertEquals("s1", response.get("submissionId").asText());
        }
    }

    @Test
    void nonSuccessfulStatusRaisesClientException() throws Exception {
        try (TestHttpServer server = TestHttpServer.start(exchange ->
                respond(exchange, 409, "{\"error\":\"conflict\"}"))) {
            MatcherHttpClient client = client(server);

            MatcherClientException error = org.junit.jupiter.api.Assertions.assertThrows(
                    MatcherClientException.class,
                    () -> client.getOrder(404L)
            );
            assertEquals(409, error.statusCode());
            assertTrue(error.responseBody().contains("conflict"));
        }
    }

    private static MatcherHttpClient client(TestHttpServer server) {
        return new MatcherHttpClient(new MatcherClientConfig(server.endpoint(), Duration.ofSeconds(2)));
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

    private record TestHttpServer(HttpServer server) implements AutoCloseable {
        static TestHttpServer start(Handler handler) throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> handler.handle(exchange));
            server.start();
            return new TestHttpServer(server);
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
