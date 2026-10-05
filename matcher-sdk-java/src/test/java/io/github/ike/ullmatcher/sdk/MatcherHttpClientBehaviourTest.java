package io.github.ike.ullmatcher.sdk;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖 MatcherHttpClient 的路由、鉴权头、超时预算与错误映射行为。
 */
final class MatcherHttpClientBehaviourTest {
    @Test
    void configuredApiKeyIsSentOnTheConfiguredHeader() throws Exception {
        AtomicReference<String> apiKey = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            apiKey.set(exchange.getRequestHeaders().getFirst("X-Tenant-Key"));
            respond(exchange, 200, "{\"status\":\"UP\"}");
        })) {
            MatcherHttpClient client = new MatcherHttpClient(new MatcherClientConfig(
                    server.endpoint(), Duration.ofSeconds(2), "secret", "X-Tenant-Key"));

            client.health();

            assertEquals("secret", apiKey.get());
        }
    }

    @Test
    void defaultApiKeyHeaderIsUsedWhenNoHeaderNameIsConfigured() throws Exception {
        AtomicReference<String> apiKey = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            apiKey.set(exchange.getRequestHeaders().getFirst(MatcherClientConfig.DEFAULT_API_KEY_HEADER));
            respond(exchange, 200, "{\"status\":\"UP\"}");
        })) {
            MatcherHttpClient client = new MatcherHttpClient(
                    new MatcherClientConfig(server.endpoint(), Duration.ofSeconds(2), "secret"));

            client.readiness();

            assertEquals("secret", apiKey.get());
        }
    }

    @Test
    void noAuthHeaderIsSentWhenApiKeyIsAbsent() throws Exception {
        AtomicReference<String> apiKey = new AtomicReference<>("present");
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            apiKey.set(exchange.getRequestHeaders().getFirst(MatcherClientConfig.DEFAULT_API_KEY_HEADER));
            respond(exchange, 200, "{\"status\":\"UP\"}");
        })) {
            MatcherHttpClient client = client(server);

            client.health();

            assertNull(apiKey.get());
        }
    }

    @Test
    void trailingSlashEndpointDoesNotDuplicateThePathSeparator() throws Exception {
        AtomicReference<String> path = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            path.set(exchange.getRequestURI().toString());
            respond(exchange, 200, "{\"status\":\"UP\"}");
        })) {
            MatcherHttpClient client = new MatcherHttpClient(new MatcherClientConfig(
                    URI.create(server.endpoint() + "/"), Duration.ofSeconds(2)));

            client.health();

            assertEquals("/api/v1/runtime/health", path.get());
        }
    }

    @Test
    void readOnlyRoutesUseGetWithoutRequestBody() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<Integer> bodyBytes = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().toString());
            bodyBytes.set(exchange.getRequestBody().readAllBytes().length);
            respond(exchange, 200, "{\"orderId\":101}");
        })) {
            MatcherHttpClient client = client(server);

            assertEquals(101, client.getOrder(101L).get("orderId").asInt());
            assertEquals("GET", method.get());
            assertEquals("/api/v1/orders/101", path.get());
            assertEquals(0, bodyBytes.get());

            client.readiness();
            assertEquals("/api/v1/runtime/readiness", path.get());
        }
    }

    @Test
    void recentOrdersClampsLimitToAtLeastOne() throws Exception {
        AtomicReference<String> path = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            path.set(exchange.getRequestURI().toString());
            respond(exchange, 200, "{\"orders\":[]}");
        })) {
            MatcherHttpClient client = client(server);

            client.recentOrders(0);
            assertEquals("/api/v1/orders?limit=1", path.get());

            client.recentOrders(-5);
            assertEquals("/api/v1/orders?limit=1", path.get());

            client.recentOrders(25);
            assertEquals("/api/v1/orders?limit=25", path.get());
        }
    }

    @Test
    void cancelOrderSendsOrderIdAndOmitsBlankIdempotencyKey() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 202, "{\"submissionId\":\"s2\"}");
        })) {
            MatcherHttpClient client = client(server);

            client.cancelOrder(new CancelOrderRequest(101L, "k9"));
            assertTrue(body.get().contains("\"orderId\":101"));
            assertTrue(body.get().contains("\"idempotencyKey\":\"k9\""));

            client.cancelOrder(new CancelOrderRequest(102L, "  "));
            assertFalse(body.get().contains("idempotencyKey"));

            client.cancelOrder(new CancelOrderRequest(103L, null));
            assertFalse(body.get().contains("idempotencyKey"));
        }
    }

    @Test
    void submitOrderIncludesTtlMillisWhenPresentAndSkipsBlankIdempotencyKey() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 202, "{\"submissionId\":\"s1\"}");
        })) {
            MatcherHttpClient client = client(server);

            client.submitOrder(new NewOrderRequest(7L, 101L, "BUY", "LIMIT", "GTD", 12L, 3L, 500L, " "));

            assertTrue(body.get().contains("\"ttlMillis\":500"));
            assertFalse(body.get().contains("idempotencyKey"));
        }
    }

    @Test
    void submitOrdersRejectsNullAndEmptyBatches() {
        MatcherHttpClient client = new MatcherHttpClient(MatcherClientConfig.localDefault());

        assertThrows(NullPointerException.class, () -> client.submitOrders(null));
        assertThrows(IllegalArgumentException.class, () -> client.submitOrders(List.of()));
    }

    @Test
    void createSnapshotPostsAnEmptyJsonDocument() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();
        AtomicReference<String> contentType = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            method.set(exchange.getRequestMethod());
            path.set(exchange.getRequestURI().toString());
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 200, "{\"lastSequence\":42}");
        })) {
            MatcherHttpClient client = client(server);

            assertEquals(42, client.createSnapshot().get("lastSequence").asInt());
            assertEquals("POST", method.get());
            assertEquals("/api/v1/admin/snapshot", path.get());
            assertEquals("application/json", contentType.get());
            assertEquals("{}", body.get());
        }
    }

    @Test
    void submissionLookupEncodesPathSegmentAndQueryValue() throws Exception {
        AtomicReference<String> path = new AtomicReference<>();
        try (TestHttpServer server = TestHttpServer.start(exchange -> {
            path.set(exchange.getRequestURI().getRawPath() + "?" + exchange.getRequestURI().getRawQuery());
            respond(exchange, 200, "{\"submissionId\":\"s 1\"}");
        })) {
            MatcherHttpClient client = client(server);

            client.getSubmission("s 1+a");
            assertTrue(path.get().startsWith("/api/v1/submissions/s%201%2Ba"), path.get());

            client.getSubmissionByIdempotencyKey("key with space");
            assertEquals("/api/v1/submissions/by-idempotency?idempotencyKey=key+with+space", path.get());
        }
    }

    @Test
    void submissionLookupRejectsBlankIdentifiers() {
        MatcherHttpClient client = new MatcherHttpClient(MatcherClientConfig.localDefault());

        assertThrows(IllegalArgumentException.class, () -> client.getSubmission(null));
        assertThrows(IllegalArgumentException.class, () -> client.getSubmission(" "));
        assertThrows(IllegalArgumentException.class, () -> client.getSubmissionByIdempotencyKey(null));
        assertThrows(IllegalArgumentException.class, () -> client.getSubmissionByIdempotencyKey(" "));
    }

    @Test
    void metricsReturnsRawTextBodyAndMapsFailuresToClientException() throws Exception {
        try (TestHttpServer server = TestHttpServer.start(exchange ->
                respondText(exchange, 200, "ull_matcher_orders_total 7"))) {
            MatcherHttpClient client = client(server);

            assertEquals("ull_matcher_orders_total 7", client.metrics());
        }

        try (TestHttpServer server = TestHttpServer.start(exchange ->
                respondText(exchange, 503, "scrape disabled"))) {
            MatcherHttpClient client = client(server);

            MatcherClientException error = assertThrows(MatcherClientException.class, client::metrics);
            assertEquals(503, error.statusCode());
            assertEquals("scrape disabled", error.responseBody());
        }
    }

    @Test
    void malformedJsonBodyIsReportedAsParseFailure() throws Exception {
        try (TestHttpServer server = TestHttpServer.start(exchange -> respond(exchange, 200, "{oops"))) {
            MatcherHttpClient client = client(server);

            MatcherClientException error = assertThrows(MatcherClientException.class, client::health);

            assertEquals("failed to parse matcher response", error.getMessage());
            assertEquals(-1, error.statusCode());
        }
    }

    @Test
    void transportIoFailureIsWrappedInClientException() {
        IOException cause = new IOException("connection reset");
        MatcherHttpClient client = new MatcherHttpClient(MatcherClientConfig.localDefault(), new ThrowingHttpClient(cause));

        MatcherClientException error = assertThrows(MatcherClientException.class, client::health);

        assertEquals("matcher request failed", error.getMessage());
        assertSame(cause, error.getCause());
        assertEquals(-1, error.statusCode());
    }

    @Test
    void interruptionIsWrappedAndReassertsTheInterruptFlag() {
        InterruptedException cause = new InterruptedException("interrupted");
        MatcherHttpClient client = new MatcherHttpClient(MatcherClientConfig.localDefault(), new ThrowingHttpClient(cause));

        MatcherClientException error = assertThrows(MatcherClientException.class, client::metrics);

        assertEquals("matcher request interrupted", error.getMessage());
        assertSame(cause, error.getCause());
        assertTrue(Thread.interrupted(), "interrupt flag must be restored for the caller");
    }

    @Test
    void constructorsRejectMissingCollaborators() {
        assertThrows(NullPointerException.class, () -> new MatcherHttpClient(null));
        assertThrows(NullPointerException.class, () -> new MatcherHttpClient(MatcherClientConfig.localDefault(), null));
    }

    @Test
    void requestTimeoutExposesTheConfiguredBudget() {
        MatcherHttpClient client = new MatcherHttpClient(
                new MatcherClientConfig(URI.create("http://127.0.0.1:1"), Duration.ofMillis(750)));

        assertEquals(Duration.ofMillis(750), client.requestTimeout());
    }

    private static MatcherHttpClient client(TestHttpServer server) {
        return new MatcherHttpClient(new MatcherClientConfig(server.endpoint(), Duration.ofSeconds(2)));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        write(exchange, status, body, "application/json; charset=utf-8");
    }

    private static void respondText(HttpExchange exchange, int status, String body) throws IOException {
        write(exchange, status, body, "text/plain; charset=utf-8");
    }

    private static void write(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", contentType);
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

    /**
     * 只用于确定性地触发 send() 的两条异常分支，不参与真实网络交互。
     */
    private static final class ThrowingHttpClient extends HttpClient {
        private final Exception failure;

        private ThrowingHttpClient(Exception failure) {
            this.failure = failure;
        }

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public SSLContext sslContext() {
            throw new UnsupportedOperationException("sslContext");
        }

        @Override
        public SSLParameters sslParameters() {
            throw new UnsupportedOperationException("sslParameters");
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
                throws IOException, InterruptedException {
            if (failure instanceof IOException io) {
                throw io;
            }
            throw (InterruptedException) failure;
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            throw new UnsupportedOperationException("sendAsync");
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                                HttpResponse.BodyHandler<T> handler,
                                                                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            throw new UnsupportedOperationException("sendAsync");
        }
    }
}
