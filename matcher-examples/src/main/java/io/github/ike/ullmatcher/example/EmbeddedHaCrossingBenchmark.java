package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.SubmissionReceipt;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.server.telemetry.MatcherNodeMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.SubmissionMetricsSnapshot;

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
 * JVM 内一主一备 direct HA crossing benchmark。
 * <p>
 * 该基准保留：
 * <pre>{@code
 * MatcherNodeService -> SubmissionTracker -> WAL -> ReplicationCoordinator -> StandbySyncService
 * }</pre>
 * 但完全绕开 HTTP/JSON 和外部进程，用于分离 HA 提交闭环成本。
 */
public final class EmbeddedHaCrossingBenchmark {
    private EmbeddedHaCrossingBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkSupport.quietBenchmarkLogging();
        Arguments parsed = Arguments.parse(args);
        Files.createDirectories(parsed.dataRoot());

        MatcherServerConfig primaryConfig = configFor(parsed, "primary", HaRole.PRIMARY);
        MatcherServerConfig standbyConfig = configFor(parsed, "standby", HaRole.STANDBY);

        try (MatcherNodeService standby = new MatcherNodeService(standbyConfig);
             MatcherNodeService primary = new MatcherNodeService(primaryConfig);
             DirectStandbyReplicator replicator = new DirectStandbyReplicator("standby", standby)) {
            standby.start();
            primary.start();
            primary.configureReplication(replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TimeUnit.SECONDS.toNanos(5));

            BenchmarkSupport.waitForReady(primary, 5_000L);

            long preloadSequence = 1_000_000L;
            for (int i = 0; i < parsed.restingOrders(); i++) {
                SubmitResult result = primary.submitNewOrder(
                        2L,
                        preloadSequence + i,
                        Side.SELL,
                        OrderType.LIMIT,
                        TimeInForce.GTC,
                        100L,
                        1L
                ).result();
                if (result != SubmitResult.ACCEPTED) {
                    throw new IllegalStateException("failed to preload resting order " + i + " result=" + result);
                }
            }
            BenchmarkSupport.waitForTrades(primary, 0L, 5_000L);

            MatcherNodeMetricsSnapshot before = primary.metricsSnapshot();
            long beforeAccepted = before.submitPathMetrics().walAcceptedTotal();
            long beforeTrades = before.matchingMetrics().tradeCount();
            long beforeProcessed = before.loopSnapshot().processedCommandCount();
            long beforeCommitted = before.submissionMetrics().committedCount();

            ExecutorService clients = Executors.newFixedThreadPool(parsed.concurrency(), Thread.ofPlatform().name("embed-ha-submit-", 0).factory());
            CountDownLatch start = new CountDownLatch(1);
            List<java.util.concurrent.Future<ResultSample>> futures = new ArrayList<>(parsed.crossingOrders());
            long orderIdBase = 2_000_000L;
            for (int i = 0; i < parsed.crossingOrders(); i++) {
                final long orderId = orderIdBase + i;
                final String idempotencyKey = "bench:" + orderId;
                futures.add(clients.submit(() -> {
                    start.await();
                    long started = System.nanoTime();
                    SubmissionReceipt receipt = primary.submitTrackedNewOrder(
                            1L,
                            orderId,
                            Side.BUY,
                            OrderType.LIMIT,
                            TimeInForce.IOC,
                            101L,
                            1L,
                            null,
                            idempotencyKey
                    ).awaitLocalReceipt(5_000L);
                    double latencyMs = (System.nanoTime() - started) / 1_000_000.0;
                    return new ResultSample(receipt.localResult(), latencyMs);
                }));
            }

            long benchmarkStarted = System.nanoTime();
            start.countDown();
            List<ResultSample> samples = new ArrayList<>(parsed.crossingOrders());
            for (var future : futures) {
                samples.add(future.get(30, TimeUnit.SECONDS));
            }
            clients.shutdown();
            clients.awaitTermination(1, TimeUnit.MINUTES);

            BenchmarkSupport.waitForTrades(primary, beforeTrades + parsed.crossingOrders(), 90_000L);
            BenchmarkSupport.waitForCommitted(primary, beforeCommitted + parsed.crossingOrders(), 90_000L);
            double elapsedSeconds = (System.nanoTime() - benchmarkStarted) / 1_000_000_000.0;

            MatcherNodeMetricsSnapshot after = primary.metricsSnapshot();
            SubmissionMetricsSnapshot submissionMetrics = after.submissionMetrics();

            long acceptedOrders = after.submitPathMetrics().walAcceptedTotal() - beforeAccepted;
            long processedCommands = after.loopSnapshot().processedCommandCount() - beforeProcessed;
            long tradeEvents = after.matchingMetrics().tradeCount() - beforeTrades;
            long matchedOrderSides = tradeEvents * 2;
            long committedSubmissions = submissionMetrics.committedCount() - beforeCommitted;
            long pendingDelta = submissionMetrics.pendingCount() - before.submissionMetrics().pendingCount();
            AtomicInteger rejected = new AtomicInteger();
            ArrayList<Double> latencies = new ArrayList<>(samples.size());
            for (ResultSample sample : samples) {
                latencies.add(sample.latencyMs());
                if (sample.result() != SubmitResult.ACCEPTED) {
                    rejected.incrementAndGet();
                }
            }

            System.out.println("{");
            System.out.println("  \"success\": " + (rejected.get() == 0 && tradeEvents == parsed.crossingOrders()) + ",");
            System.out.println("  \"scenario\": \"embedded_ha_crossing_benchmark\",");
            System.out.println("  \"topology\": \"1P1S\",");
            System.out.printf(Locale.ROOT, "  \"restingOrders\": %d,%n", parsed.restingOrders());
            System.out.printf(Locale.ROOT, "  \"crossingOrders\": %d,%n", parsed.crossingOrders());
            System.out.printf(Locale.ROOT, "  \"concurrency\": %d,%n", parsed.concurrency());
            System.out.printf(Locale.ROOT, "  \"elapsedSeconds\": %.6f,%n", elapsedSeconds);
            System.out.printf(Locale.ROOT, "  \"acceptedOrders\": %d,%n", acceptedOrders);
            System.out.printf(Locale.ROOT, "  \"processedCommands\": %d,%n", processedCommands);
            System.out.printf(Locale.ROOT, "  \"tradeEvents\": %d,%n", tradeEvents);
            System.out.printf(Locale.ROOT, "  \"matchedOrderSides\": %d,%n", matchedOrderSides);
            System.out.printf(Locale.ROOT, "  \"replicationCommittedSubmissions\": %d,%n", committedSubmissions);
            System.out.printf(Locale.ROOT, "  \"submissionPendingDelta\": %d,%n", pendingDelta);
            System.out.printf(Locale.ROOT, "  \"acceptedOrdersPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(acceptedOrders, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"processedCommandsPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(processedCommands, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"tradeEventsPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(tradeEvents, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"matchedOrderSidesPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(matchedOrderSides, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"replicationCommittedSubmissionsPerSecond\": %.2f,%n", BenchmarkSupport.perSecond(committedSubmissions, elapsedSeconds));
            System.out.printf(Locale.ROOT, "  \"meanLatencyMs\": %.2f,%n", BenchmarkSupport.mean(latencies));
            System.out.printf(Locale.ROOT, "  \"p99LatencyMs\": %.2f,%n", BenchmarkSupport.percentile(latencies, 0.99));
            System.out.printf(Locale.ROOT, "  \"primaryReplicationLastCommitMicros\": %d,%n", after.replicationMetrics().lastCommitMicros());
            System.out.printf(Locale.ROOT, "  \"standbyApplyQueueDepth\": %d,%n", standby.metricsSnapshot().standbySyncMetrics().maxObservedApplyQueueDepth());
            System.out.printf(Locale.ROOT, "  \"rejectedOrders\": %d%n", rejected.get());
            System.out.println("}");
        }
    }

    private static MatcherServerConfig configFor(Arguments parsed, String nodeId, HaRole role) {
        Path nodeRoot = parsed.dataRoot().resolve(nodeId);
        return new MatcherServerConfig(
                MatcherServerMode.DEV,
                nodeId,
                "symbol-1",
                MatcherConfig.defaults(1),
                nodeRoot.resolve("wal"),
                "symbol-1",
                parsed.walSegmentBytes(),
                parsed.durabilityMode(),
                parsed.forceBatchSize(),
                parsed.forceMaxDelayMicros(),
                nodeRoot.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 16,
                10_000,
                TimeUnit.MILLISECONDS.toNanos(500),
                0,
                "127.0.0.1",
                4,
                1 << 20,
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
                io.github.ike.ullmatcher.server.engine.TtlCancelConfig.disabled(),
                role,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                StandbySyncConfig.defaults(),
                null
        );
    }

    private record ResultSample(SubmitResult result, double latencyMs) {
    }

    private record Arguments(int restingOrders,
                             int crossingOrders,
                             int concurrency,
                             long walSegmentBytes,
                             WalDurabilityMode durabilityMode,
                             int forceBatchSize,
                             long forceMaxDelayMicros,
                             Path dataRoot) {
        private static Arguments parse(String[] args) {
            int restingOrders = 2_048;
            int crossingOrders = 2_048;
            int concurrency = 24;
            long walSegmentBytes = 64L * 1024L * 1024L;
            WalDurabilityMode durabilityMode = WalDurabilityMode.SYNC_PER_BATCH;
            int forceBatchSize = 32;
            long forceMaxDelayMicros = 500L;
            Path dataRoot = Path.of("target", "embed-ha-bench");
            List<String> tokens = new ArrayList<>(List.of(args));
            for (int i = 0; i < tokens.size(); i += 2) {
                String key = tokens.get(i);
                String value = i + 1 < tokens.size() ? tokens.get(i + 1) : "";
                switch (key) {
                    case "--resting-orders" -> restingOrders = Integer.parseInt(value);
                    case "--crossing-orders" -> crossingOrders = Integer.parseInt(value);
                    case "--concurrency" -> concurrency = Integer.parseInt(value);
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
                    walSegmentBytes,
                    durabilityMode,
                    forceBatchSize,
                    forceMaxDelayMicros,
                    dataRoot
            );
        }
    }

    private static final class DirectStandbyReplicator implements CommandReplicator, AutoCloseable {
        private final String standbyNodeId;
        private final MatcherNodeService standby;

        private DirectStandbyReplicator(String standbyNodeId, MatcherNodeService standby) {
            this.standbyNodeId = standbyNodeId;
            this.standby = standby;
        }

        @Override
        public ReplicationResult replicate(io.github.ike.ullmatcher.api.Command command, long timeoutNanos) throws java.io.IOException {
            standby.standbySyncService().replicate(copy(command), timeoutNanos);
            return new ReplicationResult(1, 1, List.of(standbyNodeId), List.of());
        }

        @Override
        public ReplicationResult replicateBatch(List<io.github.ike.ullmatcher.api.Command> commands, long timeoutNanos) throws java.io.IOException {
            ArrayList<io.github.ike.ullmatcher.api.Command> copies = new ArrayList<>(commands.size());
            for (io.github.ike.ullmatcher.api.Command command : commands) {
                copies.add(copy(command));
            }
            standby.standbySyncService().replicateBatch(copies, timeoutNanos);
            return new ReplicationResult(1, 1, List.of(standbyNodeId), List.of());
        }

        private static io.github.ike.ullmatcher.api.Command copy(io.github.ike.ullmatcher.api.Command command) {
            return switch (command.type) {
                case NEW_ORDER -> io.github.ike.ullmatcher.api.Command.newOrder(
                        command.sequence,
                        command.orderId,
                        command.userId,
                        command.symbolId,
                        decodeSide(command.side),
                        decodeOrderType(command.orderType),
                        decodeTimeInForce(command.timeInForce),
                        command.price,
                        command.quantity,
                        command.expireAtEpochMillis
                );
                case CANCEL_ORDER -> io.github.ike.ullmatcher.api.Command.cancel(
                        command.sequence,
                        command.orderId,
                        command.symbolId
                );
                case SNAPSHOT_MARKER -> io.github.ike.ullmatcher.api.Command.snapshotMarker(
                        command.sequence,
                        command.symbolId
                );
                case SHUTDOWN -> io.github.ike.ullmatcher.api.Command.shutdown(command.sequence);
            };
        }

        private static Side decodeSide(byte code) {
            return code == Side.BUY.code ? Side.BUY : Side.SELL;
        }

        private static OrderType decodeOrderType(byte code) {
            for (OrderType type : OrderType.values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("unknown orderType code " + code);
        }

        private static TimeInForce decodeTimeInForce(byte code) {
            for (TimeInForce tif : TimeInForce.values()) {
                if (tif.code == code) {
                    return tif;
                }
            }
            throw new IllegalArgumentException("unknown timeInForce code " + code);
        }

        @Override
        public void close() {
        }
    }
}
