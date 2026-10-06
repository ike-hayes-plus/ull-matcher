package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.api.HttpApiServer;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.telemetry.MatcherNodeMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.ReadinessSnapshot;

import io.github.ike.ullmatcher.net.MatcherHttpTransport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单节点 server crossing benchmark。
 * <p>
 * 该基准保留：
 * <pre>{@code
 * HttpApiServer -> MatcherNodeService -> SubmissionTracker -> WAL -> MatchLoop
 * }</pre>
 * 但完全去掉 HA 复制链，用于拆分 HTTP/JSON 与 HA 提交闭环成本。
 */
public final class SingleNodeServerCrossingBenchmark {
    private SingleNodeServerCrossingBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkSupport.quietBenchmarkLogging();
        Arguments parsed = Arguments.parse(args);
        Files.createDirectories(parsed.dataRoot());

        MatcherServerConfig config = MatcherServerConfig.defaults("node-a", 1, parsed.dataRoot()).toBuilder()
                .httpWorkerThreads(parsed.httpWorkerThreads())
                .httpSubmitEndpointMaxConcurrentRequests(parsed.httpSubmitEndpointBudget())
                .walSegmentSizeBytes(parsed.walSegmentBytes())
                .walDurabilityMode(parsed.durabilityMode())
                .walForceBatchSize(parsed.forceBatchSize())
                .walForceMaxDelayMicros(parsed.forceMaxDelayMicros())
                .build();

        try (MatcherNodeService nodeService = new MatcherNodeService(config);
             HttpApiServer server = new HttpApiServer(
                     0,
                     config.httpBindHost(),
                     config.httpWorkerThreads(),
                     config.httpMaxBodyBytes(),
                     config.httpMaxConcurrentRequests(),
                     config.httpRequestTimeoutMillis(),
                     config.httpWriteMaxConcurrentRequests(),
                     config.httpReadMaxConcurrentRequests(),
                     config.httpAdminMaxConcurrentRequests(),
                     config.httpWriteTimeoutMillis(),
                     config.httpReadTimeoutMillis(),
                     config.httpAdminTimeoutMillis(),
                     config.httpSubmitEndpointMaxConcurrentRequests(),
                     config.httpCancelEndpointMaxConcurrentRequests(),
                     config.httpSnapshotEndpointMaxConcurrentRequests(),
                     config.httpReadinessEndpointMaxConcurrentRequests(),
                     config.httpMetricsEndpointMaxConcurrentRequests(),
                     config.shardKey(),
                     config.writeAdmissionPolicyConfig(),
                     nodeService,
                     new GrpcTransportMetrics(),
                     () -> new ClusterSupervisorMetricsSnapshot(
                             0L,
                             0L,
                             null,
                             null,
                             Map.of(),
                             List.of(),
                             "IDLE",
                             "",
                             TransportMetricsSnapshot.none("NONE")
                     ),
                     () -> new ReadinessSnapshot(
                             true,
                             true,
                             true,
                             false,
                             false,
                             false,
                             0L,
                             0L,
                             0L,
                             "",
                             "READY",
                             List.of(),
                             null,
                             null,
                             "NONE",
                             "",
                             "",
                             0L,
                             0L,
                             0L,
                             0L,
                             "STABLE",
                             "transport policy is stable",
                             "DISABLED",
                             "sequence reconciliation is disabled for this transport mode",
                             "ready"
                     ))) {
            nodeService.start();
            server.start();
            BenchmarkSupport.waitForReady(nodeService, 5_000L);

            long preloadStartOrderId = 1_000_000L;
            for (int i = 0; i < parsed.restingOrders(); i++) {
                var result = nodeService.submitNewOrder(
                        2L,
                        preloadStartOrderId + i,
                        io.github.ike.ullmatcher.api.Side.SELL,
                        io.github.ike.ullmatcher.api.OrderType.LIMIT,
                        io.github.ike.ullmatcher.api.TimeInForce.GTC,
                        100L,
                        1L
                ).result();
                if (result != io.github.ike.ullmatcher.hft.SubmitResult.ACCEPTED) {
                    throw new IllegalStateException("failed to preload resting order " + i + " result=" + result);
                }
            }

            BenchmarkSupport.waitForTrades(nodeService, 0L, 5_000L);

            HttpClient client = MatcherHttpTransport.newClientBuilder(Duration.ofSeconds(5)).build();
            boolean batchMode = parsed.batchSize() > 1;
            URI ordersUri = URI.create(batchMode
                    ? "http://127.0.0.1:" + server.port() + "/api/v1/orders/batch"
                    : "http://127.0.0.1:" + server.port() + "/api/v1/orders");

            MatcherNodeMetricsSnapshot before = nodeService.metricsSnapshot();
            long beforeAccepted = before.submitPathMetrics().walAcceptedTotal();
            long beforeProcessed = before.loopSnapshot().processedCommandCount();
            long beforeTrades = before.matchingMetrics().tradeCount();
            long beforeCommitted = before.submissionMetrics().committedCount();

            ExecutorService workers = Executors.newFixedThreadPool(
                    parsed.concurrency(),
                    Thread.ofVirtual().name("single-node-http-bench-", 0).factory());
            CountDownLatch start = new CountDownLatch(1);
            int httpRequests = batchMode
                    ? (parsed.crossingOrders() + parsed.batchSize() - 1) / parsed.batchSize()
                    : parsed.crossingOrders();
            List<java.util.concurrent.Future<ResultSample>> futures = new ArrayList<>(httpRequests);
            long orderIdBase = 2_000_000L;
            for (int batchIndex = 0; batchIndex < httpRequests; batchIndex++) {
                final int firstOrderIndex = batchIndex * parsed.batchSize();
                final int ordersInBatch = Math.min(parsed.batchSize(), parsed.crossingOrders() - firstOrderIndex);
                final String payload = batchMode
                        ? batchRequestBody(orderIdBase + firstOrderIndex, ordersInBatch)
                        : requestBody(orderIdBase + firstOrderIndex);
                futures.add(workers.submit(() -> {
                    start.await();
                    long started = System.nanoTime();
                    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                            .uri(ordersUri)
                            .timeout(Duration.ofSeconds(5))
                            .header("Content-Type", "application/json");
                    if (!batchMode) {
                        requestBuilder.header("Idempotency-Key", "bench:" + (orderIdBase + firstOrderIndex));
                    }
                    HttpRequest request = requestBuilder
                            .POST(HttpRequest.BodyPublishers.ofString(payload))
                            .build();
                    HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                    double latencyMs = (System.nanoTime() - started) / 1_000_000.0;
                    return new ResultSample(response.statusCode(), latencyMs);
                }));
            }

            long benchmarkStarted = System.nanoTime();
            start.countDown();
            workers.shutdown();
            List<ResultSample> samples = new ArrayList<>(parsed.crossingOrders());
            for (var future : futures) {
                samples.add(future.get(30, TimeUnit.SECONDS));
            }
            workers.awaitTermination(1, TimeUnit.MINUTES);

            BenchmarkSupport.waitForTrades(nodeService, beforeTrades + parsed.crossingOrders(), 90_000L);
            BenchmarkSupport.waitForCommitted(nodeService, beforeCommitted + parsed.crossingOrders(), 30_000L);
            double elapsedSeconds = (System.nanoTime() - benchmarkStarted) / 1_000_000_000.0;

            MatcherNodeMetricsSnapshot after = nodeService.metricsSnapshot();
            long acceptedOrders = after.submitPathMetrics().walAcceptedTotal() - beforeAccepted;
            long processedCommands = after.loopSnapshot().processedCommandCount() - beforeProcessed;
            long tradeEvents = after.matchingMetrics().tradeCount() - beforeTrades;
            long committedSubmissions = after.submissionMetrics().committedCount() - beforeCommitted;
            long pendingDelta = after.submissionMetrics().pendingCount() - before.submissionMetrics().pendingCount();
            List<Double> latencies = new ArrayList<>(samples.size());
            AtomicInteger rejected = new AtomicInteger();
            for (ResultSample sample : samples) {
                latencies.add(sample.latencyMs());
                if (!Set.of(200, 202).contains(sample.statusCode())) {
                    rejected.incrementAndGet();
                }
            }

            System.out.println("{");
            System.out.println("  \"success\": " + (rejected.get() == 0 && tradeEvents == parsed.crossingOrders()) + ",");
            System.out.println("  \"scenario\": \"single_node_server_crossing_benchmark\",");
            System.out.println("  \"topology\": \"single_node_http\",");
            BenchmarkSupport.printJsonMetadata();
            System.out.printf(Locale.ROOT, "  \"restingOrders\": %d,%n", parsed.restingOrders());
            System.out.printf(Locale.ROOT, "  \"crossingOrders\": %d,%n", parsed.crossingOrders());
            System.out.printf(Locale.ROOT, "  \"concurrency\": %d,%n", parsed.concurrency());
            System.out.printf(Locale.ROOT, "  \"httpBatchSize\": %d,%n", parsed.batchSize());
            System.out.printf(Locale.ROOT, "  \"httpRequests\": %d,%n", httpRequests);
            System.out.printf(Locale.ROOT, "  \"elapsedSeconds\": %.6f,%n", elapsedSeconds);
            System.out.printf(Locale.ROOT, "  \"acceptedOrders\": %d,%n", acceptedOrders);
            System.out.printf(Locale.ROOT, "  \"processedCommands\": %d,%n", processedCommands);
            System.out.printf(Locale.ROOT, "  \"tradeEvents\": %d,%n", tradeEvents);
            System.out.printf(Locale.ROOT, "  \"matchedOrderSides\": %d,%n", tradeEvents * 2);
            System.out.printf(Locale.ROOT, "  \"replicationCommittedSubmissions\": %d,%n", committedSubmissions);
            System.out.printf(Locale.ROOT, "  \"submissionPendingDelta\": %d,%n", pendingDelta);
            System.out.printf(Locale.ROOT, "  \"acceptedOrdersPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(acceptedOrders, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"processedCommandsPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(processedCommands, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"tradeEventsPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(tradeEvents, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"matchedOrderSidesPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(tradeEvents * 2L, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"replicationCommittedSubmissionsPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(committedSubmissions, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"meanLatencyMs\": %.2f,%n", BenchmarkSupport.mean(latencies));
            System.out.printf(Locale.ROOT, "  \"p99LatencyMs\": %.2f,%n", BenchmarkSupport.percentile(latencies, 0.99));
            System.out.printf(Locale.ROOT, "  \"rejectedOrders\": %d%n", rejected.get());
            System.out.println("}");
        }
    }

    private static String requestBody(long orderId) {
        return "{\"userId\":1,\"orderId\":" + orderId
                + ",\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"IOC\",\"price\":101,\"quantity\":1}";
    }

    private static String batchRequestBody(long firstOrderId, int count) {
        StringBuilder payload = new StringBuilder(count * 96 + 16);
        payload.append("{\"orders\":[");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                payload.append(',');
            }
            long orderId = firstOrderId + i;
            payload.append("{\"userId\":1,\"orderId\":").append(orderId)
                    .append(",\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"IOC\",\"price\":101,\"quantity\":1}");
        }
        payload.append("]}");
        return payload.toString();
    }

    private record ResultSample(int statusCode, double latencyMs) {
    }

    private record Arguments(int restingOrders,
                             int crossingOrders,
                             int concurrency,
                             int batchSize,
                             int httpSubmitEndpointBudget,
                             int httpWorkerThreads,
                             long walSegmentBytes,
                             WalDurabilityMode durabilityMode,
                             int forceBatchSize,
                             long forceMaxDelayMicros,
                             Path dataRoot) {
        private static Arguments parse(String[] args) {
            int restingOrders = 2_048;
            int crossingOrders = 2_048;
            int concurrency = 24;
            int batchSize = 1;
            int httpSubmitEndpointBudget = MatcherServerConfig.DEFAULT_HTTP_SUBMIT_ENDPOINT_MAX_CONCURRENT_REQUESTS;
            int httpWorkerThreads = MatcherServerConfig.defaultHttpWorkerThreads();
            long walSegmentBytes = 64L * 1024L * 1024L;
            WalDurabilityMode durabilityMode = WalDurabilityMode.SYNC_PER_BATCH;
            int forceBatchSize = 32;
            long forceMaxDelayMicros = 500L;
            Path dataRoot = Path.of("target", "single-node-server-bench");
            List<String> tokens = new ArrayList<>(List.of(args));
            for (int i = 0; i < tokens.size(); i += 2) {
                String key = tokens.get(i);
                String value = i + 1 < tokens.size() ? tokens.get(i + 1) : "";
                switch (key) {
                    case "--resting-orders" -> restingOrders = Integer.parseInt(value);
                    case "--crossing-orders" -> crossingOrders = Integer.parseInt(value);
                    case "--concurrency" -> concurrency = Integer.parseInt(value);
                    case "--batch-size" -> batchSize = Integer.parseInt(value);
                    case "--http-submit-endpoint-budget" -> httpSubmitEndpointBudget = Integer.parseInt(value);
                    case "--http-worker-threads" -> httpWorkerThreads = Integer.parseInt(value);
                    case "--wal-segment-bytes" -> walSegmentBytes = Long.parseLong(value);
                    case "--durability-mode" -> durabilityMode = WalDurabilityMode.valueOf(value);
                    case "--force-batch-size" -> forceBatchSize = Integer.parseInt(value);
                    case "--force-max-delay-micros" -> forceMaxDelayMicros = Long.parseLong(value);
                    case "--data-root" -> dataRoot = Path.of(value);
                    default -> throw new IllegalArgumentException("unknown argument: " + key);
                }
            }
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batch-size must be positive");
            }
            return new Arguments(
                    restingOrders,
                    crossingOrders,
                    concurrency,
                    batchSize,
                    httpSubmitEndpointBudget,
                    httpWorkerThreads,
                    walSegmentBytes,
                    durabilityMode,
                    forceBatchSize,
                    forceMaxDelayMicros,
                    dataRoot
            );
        }
    }
}
