package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.api.BinaryOrderIngressServer;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.server.telemetry.MatcherNodeMetricsSnapshot;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 二进制长连接 crossing benchmark。
 * <p>
 * 该基准保留单节点提交流水线，但把 HTTP/JSON 替换为固定帧二进制协议：
 * <pre>{@code
 * long-lived socket -> batched binary frame -> MatcherNodeService.submitNewOrderBatch(...)
 * }</pre>
 */
public final class BinaryIngressCrossingBenchmark {
    private static final int REQUEST_MAGIC = 0x554C4C42;
    private static final int RESPONSE_MAGIC = 0x554C4C52;
    private static final short PROTOCOL_VERSION = 1;
    private static final short FRAME_TYPE_NEW_ORDER_BATCH = 1;
    private static final short FRAME_TYPE_BATCH_RESULT = 101;
    private static final int FRAME_HEADER_BYTES = 16;
    private static final int REQUEST_RECORD_BYTES = 48;
    private static final int RESPONSE_RECORD_BYTES = 24;

    private BinaryIngressCrossingBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkSupport.quietBenchmarkLogging();
        Arguments parsed = Arguments.parse(args);
        Files.createDirectories(parsed.dataRoot());

        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "symbol-1",
                MatcherConfig.defaults(1),
                parsed.dataRoot().resolve("wal"),
                "symbol-1",
                parsed.walSegmentBytes(),
                parsed.durabilityMode(),
                parsed.forceBatchSize(),
                parsed.forceMaxDelayMicros(),
                parsed.dataRoot().resolve("snapshots").resolve("symbol-1.snap"),
                1 << 16,
                10_000,
                TimeUnit.MILLISECONDS.toNanos(500),
                0,
                "127.0.0.1",
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                1 << 20,
                512,
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

        try (MatcherNodeService nodeService = new MatcherNodeService(config);
             BinaryOrderIngressServer ingressServer = new BinaryOrderIngressServer("127.0.0.1", 0, parsed.batchSize(), nodeService)) {
            nodeService.start();
            ingressServer.start();
            BenchmarkSupport.waitForReady(nodeService, 5_000L);

            long preloadStartOrderId = 1_000_000L;
            for (int i = 0; i < parsed.restingOrders(); i++) {
                var result = nodeService.submitNewOrder(
                        2L,
                        preloadStartOrderId + i,
                        Side.SELL,
                        OrderType.LIMIT,
                        TimeInForce.GTC,
                        100L,
                        1L
                ).result();
                if (result != io.github.ike.ullmatcher.hft.SubmitResult.ACCEPTED) {
                    throw new IllegalStateException("failed to preload resting order " + i + " result=" + result);
                }
            }

            MatcherNodeMetricsSnapshot before = nodeService.metricsSnapshot();
            long beforeAccepted = before.submitPathMetrics().walAcceptedTotal();
            long beforeProcessed = before.loopSnapshot().processedCommandCount();
            long beforeTrades = before.matchingMetrics().tradeCount();

            CountDownLatch start = new CountDownLatch(1);
            ExecutorService workers = Executors.newFixedThreadPool(parsed.concurrency(), Thread.ofPlatform().name("binary-bench-", 0).factory());
            List<java.util.concurrent.Future<WorkerResult>> futures = new ArrayList<>(parsed.concurrency());
            long ordersPerWorker = parsed.crossingOrders() / parsed.concurrency();
            long remainder = parsed.crossingOrders() % parsed.concurrency();
            long nextOrderId = 2_000_000L;
            for (int worker = 0; worker < parsed.concurrency(); worker++) {
                long workerOrders = ordersPerWorker + (worker < remainder ? 1 : 0);
                long orderIdBase = nextOrderId;
                nextOrderId += workerOrders;
                futures.add(workers.submit(() -> runWorker(start, ingressServer.port(), parsed.batchSize(), orderIdBase, workerOrders)));
            }

            long started = System.nanoTime();
            start.countDown();
            workers.shutdown();
            ArrayList<Double> latencies = new ArrayList<>(parsed.crossingOrders());
            AtomicInteger rejected = new AtomicInteger();
            for (var future : futures) {
                WorkerResult result = future.get(30, TimeUnit.SECONDS);
                latencies.addAll(result.latenciesMs());
                rejected.addAndGet(result.rejected());
            }
            workers.awaitTermination(1, TimeUnit.MINUTES);

            BenchmarkSupport.waitForTrades(nodeService, beforeTrades + parsed.crossingOrders(), 90_000L);
            double elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;

            MatcherNodeMetricsSnapshot after = nodeService.metricsSnapshot();
            long acceptedOrders = after.submitPathMetrics().walAcceptedTotal() - beforeAccepted;
            long processedCommands = after.loopSnapshot().processedCommandCount() - beforeProcessed;
            long tradeEvents = after.matchingMetrics().tradeCount() - beforeTrades;
            long committedSubmissions = acceptedOrders;

            System.out.println("{");
            System.out.println("  \"success\": " + (rejected.get() == 0 && tradeEvents == parsed.crossingOrders()) + ",");
            System.out.println("  \"scenario\": \"binary_ingress_crossing_benchmark\",");
            System.out.println("  \"topology\": \"single_node_binary\",");
            BenchmarkSupport.printJsonMetadata();
            System.out.printf(Locale.ROOT, "  \"restingOrders\": %d,%n", parsed.restingOrders());
            System.out.printf(Locale.ROOT, "  \"crossingOrders\": %d,%n", parsed.crossingOrders());
            System.out.printf(Locale.ROOT, "  \"concurrency\": %d,%n", parsed.concurrency());
            System.out.printf(Locale.ROOT, "  \"batchSize\": %d,%n", parsed.batchSize());
            System.out.printf(Locale.ROOT, "  \"elapsedSeconds\": %.6f,%n", elapsedSeconds);
            System.out.printf(Locale.ROOT, "  \"acceptedOrders\": %d,%n", acceptedOrders);
            System.out.printf(Locale.ROOT, "  \"processedCommands\": %d,%n", processedCommands);
            System.out.printf(Locale.ROOT, "  \"tradeEvents\": %d,%n", tradeEvents);
            System.out.printf(Locale.ROOT, "  \"matchedOrderSides\": %d,%n", tradeEvents * 2);
            System.out.printf(Locale.ROOT, "  \"replicationCommittedSubmissions\": %d,%n", committedSubmissions);
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

    private static WorkerResult runWorker(CountDownLatch start,
                                          int port,
                                          int batchSize,
                                          long orderIdBase,
                                          long orderCount) throws Exception {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.configureBlocking(true);
            channel.connect(new InetSocketAddress("127.0.0.1", port));
            ArrayList<Double> latencies = new ArrayList<>((int) orderCount);
            ByteBuffer request = ByteBuffer.allocateDirect(FRAME_HEADER_BYTES + batchSize * REQUEST_RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
            ByteBuffer responseHeader = ByteBuffer.allocateDirect(FRAME_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            ByteBuffer responseBody = ByteBuffer.allocateDirect(batchSize * RESPONSE_RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
            int rejected = 0;
            start.await();
            long sent = 0L;
            while (sent < orderCount) {
                int frameCount = (int) Math.min(batchSize, orderCount - sent);
                request.clear();
                request.putInt(REQUEST_MAGIC);
                request.putShort(PROTOCOL_VERSION);
                request.putShort(FRAME_TYPE_NEW_ORDER_BATCH);
                request.putInt(frameCount);
                request.putInt(frameCount * REQUEST_RECORD_BYTES);
                for (int i = 0; i < frameCount; i++) {
                    long orderId = orderIdBase + sent + i;
                    request.putLong(1L);
                    request.putLong(orderId);
                    request.putLong(101L);
                    request.putLong(1L);
                    request.putLong(-1L);
                    request.put((byte) 'B');
                    request.put((byte) 'L');
                    request.put((byte) 'I');
                    request.put((byte) 0);
                    request.putInt(0);
                }
                request.flip();
                long started = System.nanoTime();
                BenchmarkSupport.writeFully(channel, request);
                BenchmarkSupport.readFully(channel, responseHeader);
                responseHeader.flip();
                int magic = responseHeader.getInt();
                short version = responseHeader.getShort();
                short frameType = responseHeader.getShort();
                int responseCount = responseHeader.getInt();
                int payloadBytes = responseHeader.getInt();
                responseHeader.clear();
                if (magic != RESPONSE_MAGIC || version != PROTOCOL_VERSION || frameType != FRAME_TYPE_BATCH_RESULT || responseCount != frameCount) {
                    throw new IOException("binary ingress response mismatch");
                }
                responseBody.clear();
                responseBody.limit(payloadBytes);
                BenchmarkSupport.readFully(channel, responseBody);
                responseBody.flip();
                double frameLatencyMs = (System.nanoTime() - started) / 1_000_000.0;
                double perOrderLatencyMs = frameLatencyMs / frameCount;
                for (int i = 0; i < frameCount; i++) {
                    responseBody.getLong();
                    responseBody.getLong();
                    int status = responseBody.getInt();
                    responseBody.getInt();
                    if (status != 0) {
                        rejected++;
                    }
                    latencies.add(perOrderLatencyMs);
                }
                sent += frameCount;
            }
            return new WorkerResult(latencies, rejected);
        }
    }

    private record WorkerResult(List<Double> latenciesMs, int rejected) {
    }

    private record Arguments(int restingOrders,
                             int crossingOrders,
                             int concurrency,
                             int batchSize,
                             long walSegmentBytes,
                             WalDurabilityMode durabilityMode,
                             int forceBatchSize,
                             long forceMaxDelayMicros,
                             Path dataRoot) {
        private static Arguments parse(String[] args) {
            int restingOrders = 2_048;
            int crossingOrders = 2_048;
            int concurrency = 24;
            int batchSize = 64;
            long walSegmentBytes = 64L * 1024L * 1024L;
            WalDurabilityMode durabilityMode = WalDurabilityMode.SYNC_PER_BATCH;
            int forceBatchSize = 32;
            long forceMaxDelayMicros = 500L;
            Path dataRoot = Path.of("target", "binary-ingress-bench");
            List<String> tokens = new ArrayList<>(List.of(args));
            for (int i = 0; i < tokens.size(); i += 2) {
                String key = tokens.get(i);
                String value = i + 1 < tokens.size() ? tokens.get(i + 1) : "";
                switch (key) {
                    case "--resting-orders" -> restingOrders = Integer.parseInt(value);
                    case "--crossing-orders" -> crossingOrders = Integer.parseInt(value);
                    case "--concurrency" -> concurrency = Integer.parseInt(value);
                    case "--batch-size" -> batchSize = Integer.parseInt(value);
                    case "--wal-segment-bytes" -> walSegmentBytes = Long.parseLong(value);
                    case "--durability-mode" -> durabilityMode = WalDurabilityMode.valueOf(value);
                    case "--force-batch-size" -> forceBatchSize = Integer.parseInt(value);
                    case "--force-max-delay-micros" -> forceMaxDelayMicros = Long.parseLong(value);
                    case "--data-root" -> dataRoot = Path.of(value);
                    default -> throw new IllegalArgumentException("unknown argument: " + key);
                }
            }
            return new Arguments(
                    restingOrders,
                    crossingOrders,
                    concurrency,
                    batchSize,
                    walSegmentBytes,
                    durabilityMode,
                    forceBatchSize,
                    forceMaxDelayMicros,
                    dataRoot
            );
        }
    }
}
