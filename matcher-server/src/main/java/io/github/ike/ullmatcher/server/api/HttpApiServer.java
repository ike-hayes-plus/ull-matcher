package io.github.ike.ullmatcher.server.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.OrderStateView;
import io.github.ike.ullmatcher.server.engine.SubmissionPhase;
import io.github.ike.ullmatcher.server.engine.SubmissionReceipt;
import io.github.ike.ullmatcher.server.engine.SubmissionView;
import io.github.ike.ullmatcher.server.telemetry.MatcherNodeMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.ReadinessSnapshot;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Headers;
import io.undertow.util.AttachmentKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class HttpApiServer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(HttpApiServer.class);
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    private static final String METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";
    private static final String ROUTE_ORDERS = "/api/v1/orders";
    private static final String ROUTE_ORDER_BY_ID = "/api/v1/orders/{orderId}";
    private static final String ROUTE_CANCEL = "/api/v1/orders/cancel";
    private static final String ROUTE_SUBMISSION = "/api/v1/submissions/{submissionId}";
    private static final String ROUTE_SUBMISSION_BY_KEY = "/api/v1/submissions/by-idempotency";
    private static final String ROUTE_SNAPSHOT = "/api/v1/admin/snapshot";
    private static final String ROUTE_HEALTH = "/api/v1/runtime/health";
    private static final String ROUTE_STATE = "/api/v1/runtime/state";
    private static final String ROUTE_READINESS = "/api/v1/runtime/readiness";
    private static final String ROUTE_METRICS = "/metrics";
    private static final int DEFAULT_RECENT_ORDERS_LIMIT = 50;
    private static final int METRICS_BUFFER_INITIAL_CAPACITY = 512;
    private static final long CLOSE_TIMEOUT_SECONDS = 5L;
    private static final long[] ENDPOINT_LATENCY_BUCKETS_MILLIS = {10L, 50L, 100L, 250L, 500L, 1_000L, 2_500L, 5_000L};
    private static final AttachmentKey<ResponseCommitGuard> RESPONSE_GUARD = AttachmentKey.create(ResponseCommitGuard.class);

    private final MatcherNodeService nodeService;
    private final GrpcTransportMetrics grpcMetrics;
    private final Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier;
    private final Supplier<ReadinessSnapshot> readinessSupplier;
    private final int maxBodyBytes;
    private final int maxConcurrentRequests;
    private final long requestTimeoutMillis;
    private final String bindHost;
    private final int requestedPort;
    private final RouteBudget writeBudget;
    private final RouteBudget readBudget;
    private final RouteBudget adminBudget;
    private final WriteAdmissionController writeAdmissionController;
    private final EndpointBudget submitBudget;
    private final EndpointBudget cancelBudget;
    private final EndpointBudget snapshotBudget;
    private final EndpointBudget readinessBudget;
    private final EndpointBudget metricsBudget;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectReader newOrderRequestReader = objectMapper.readerFor(NewOrderRequest.class);
    private final ObjectReader cancelOrderRequestReader = objectMapper.readerFor(CancelOrderRequest.class);
    private final ThreadPoolExecutor requestExecutor;
    private final int requestExecutorQueueCapacity;
    private final Semaphore requestSlots;
    private final AtomicLong globalOverloadCount = new AtomicLong();
    private final Map<String, EndpointStats> endpointStats = new ConcurrentHashMap<>();
    private final Undertow server;
    private final AtomicInteger boundPort = new AtomicInteger();

    public HttpApiServer(int port, String bindHost, int workerThreads, int maxBodyBytes, int maxConcurrentRequests, long requestTimeoutMillis,
                         int writeMaxConcurrentRequests, int readMaxConcurrentRequests, int adminMaxConcurrentRequests,
                         long writeTimeoutMillis, long readTimeoutMillis, long adminTimeoutMillis,
                         int submitEndpointMaxConcurrentRequests, int cancelEndpointMaxConcurrentRequests,
                         int snapshotEndpointMaxConcurrentRequests, int readinessEndpointMaxConcurrentRequests,
                         int metricsEndpointMaxConcurrentRequests,
                         String shardKey, WriteAdmissionPolicyConfig writeAdmissionPolicyConfig,
                         MatcherNodeService nodeService,
                         GrpcTransportMetrics grpcMetrics,
                         Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier,
                         Supplier<ReadinessSnapshot> readinessSupplier) {
        this.nodeService = Objects.requireNonNull(nodeService, "nodeService");
        this.grpcMetrics = Objects.requireNonNull(grpcMetrics, "grpcMetrics");
        this.clusterMetricsSupplier = Objects.requireNonNull(clusterMetricsSupplier, "clusterMetricsSupplier");
        this.readinessSupplier = Objects.requireNonNull(readinessSupplier, "readinessSupplier");
        this.maxBodyBytes = maxBodyBytes;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost");
        this.requestedPort = port;
        int requestThreads = Math.max(2, workerThreads);
        this.writeBudget = new RouteBudget("write", writeMaxConcurrentRequests, new Semaphore(writeMaxConcurrentRequests), writeTimeoutMillis);
        this.readBudget = new RouteBudget("read", readMaxConcurrentRequests, new Semaphore(readMaxConcurrentRequests), readTimeoutMillis);
        this.adminBudget = new RouteBudget("admin", adminMaxConcurrentRequests, new Semaphore(adminMaxConcurrentRequests), adminTimeoutMillis);
        this.writeAdmissionController = new WriteAdmissionController(shardKey, writeAdmissionPolicyConfig);
        this.submitBudget = new EndpointBudget("submit_order", submitEndpointMaxConcurrentRequests, new Semaphore(submitEndpointMaxConcurrentRequests));
        this.cancelBudget = new EndpointBudget("cancel_order", cancelEndpointMaxConcurrentRequests, new Semaphore(cancelEndpointMaxConcurrentRequests));
        this.snapshotBudget = new EndpointBudget("create_snapshot", snapshotEndpointMaxConcurrentRequests, new Semaphore(snapshotEndpointMaxConcurrentRequests));
        this.readinessBudget = new EndpointBudget("runtime_readiness", readinessEndpointMaxConcurrentRequests, new Semaphore(readinessEndpointMaxConcurrentRequests));
        this.metricsBudget = new EndpointBudget("metrics", metricsEndpointMaxConcurrentRequests, new Semaphore(metricsEndpointMaxConcurrentRequests));
        this.requestExecutorQueueCapacity = Math.max(
                requestThreads,
                Math.min(maxConcurrentRequests, requestThreads * 2)
        );
        this.requestExecutor = new ThreadPoolExecutor(
                requestThreads,
                requestThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(requestExecutorQueueCapacity),
                Thread.ofPlatform().name("matcher-http-" + port + "-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        this.requestExecutor.prestartAllCoreThreads();
        this.requestSlots = new Semaphore(maxConcurrentRequests);
        RoutingHandler routes = Handlers.routing()
                .get(ROUTE_ORDERS, blocking("recent_orders", "recent orders", readBudget, null, this::handleRecentOrders))
                .get(ROUTE_ORDER_BY_ID, blocking("get_order", "get order", readBudget, null, this::handleGetOrder))
                .post(ROUTE_ORDERS, directBlocking("submit_order", "submit order", writeBudget, submitBudget, this::handleSubmitOrder))
                .post(ROUTE_CANCEL, directBlocking("cancel_order", "cancel order", writeBudget, cancelBudget, this::handleCancelOrder))
                .get(ROUTE_SUBMISSION, blocking("get_submission", "get submission", readBudget, null, this::handleGetSubmission))
                .get(ROUTE_SUBMISSION_BY_KEY, blocking("get_submission_by_key", "get submission by idempotency key", readBudget, null, this::handleGetSubmissionByKey))
                .post(ROUTE_SNAPSHOT, blocking("create_snapshot", "create snapshot", adminBudget, snapshotBudget, this::handleCreateSnapshot))
                .get(ROUTE_HEALTH, blocking("runtime_health", "runtime health", readBudget, null, this::handleHealth))
                .get(ROUTE_STATE, blocking("runtime_state", "runtime state", readBudget, null, this::handleHealth))
                .get(ROUTE_READINESS, blocking("runtime_readiness", "runtime readiness", readBudget, readinessBudget, this::handleReadiness))
                .get(ROUTE_METRICS, blocking("metrics", "metrics", readBudget, metricsBudget, this::handleMetrics))
                .setFallbackHandler(this::handleNotFound);
        int ioThreads = Math.max(2, Math.min(workerThreads, Runtime.getRuntime().availableProcessors()));
        this.server = Undertow.builder()
                .setIoThreads(ioThreads)
                .setWorkerThreads(Math.max(workerThreads, ioThreads))
                .addHttpListener(port, bindHost)
                .setHandler(routes)
                .build();
    }

    public void start() {
        server.start();
        int actualPort = requestedPort;
        if (!server.getListenerInfo().isEmpty() && server.getListenerInfo().getFirst().getAddress() instanceof java.net.InetSocketAddress address) {
            actualPort = address.getPort();
        }
        boundPort.set(actualPort);
    }

    public int port() {
        return boundPort.get() == 0 ? requestedPort : boundPort.get();
    }

    @Override
    public void close() {
        server.stop();
        requestExecutor.shutdownNow();
        try {
            requestExecutor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleSubmitOrder(HttpServerExchange exchange) throws IOException {
        NewOrderRequest request = readNewOrderRequest(exchange);
        try {
            WriteAdmissionController.Admission admission = writeAdmissionController.acquireForSubmit(exchange, request.userId());
            try (admission) {
                String idempotencyKey = resolveIdempotencyKey(exchange, request.idempotencyKey(), defaultOrderIdempotencyKey(request.userId(), request.orderId()));
                var handle = nodeService.submitTrackedNewOrder(
                        request.userId(),
                        request.orderId(),
                        parseEnum(request.side(), Side.class, "side"),
                        parseEnum(request.orderType(), OrderType.class, "orderType"),
                        parseEnum(request.timeInForce(), TimeInForce.class, "timeInForce"),
                        request.price(),
                        request.quantity(),
                        request.ttlMillis(),
                        idempotencyKey
                );
                SubmissionReceipt receipt = handle.awaitLocalReceipt(writeBudget.timeoutMillis());
                writeSubmissionResponse(exchange, receipt);
            }
        } catch (IOException e) {
            handleServiceFailure(exchange, "submit order", e);
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "submit order", e);
        }
    }

    private void handleGetOrder(HttpServerExchange exchange) throws IOException {
        String orderIdText = exchange.getQueryParameters().containsKey("orderId")
                ? exchange.getQueryParameters().get("orderId").getFirst()
                : exchange.getPathParameters().get("orderId").getFirst();
        if (orderIdText == null || orderIdText.isBlank()) {
            writeJson(exchange, 400, Map.of("error", "missing orderId"));
            return;
        }
        try {
            long orderId = Long.parseLong(orderIdText);
            OrderStateView state = nodeService.orderState(orderId);
            if (state == null) {
                writeJson(exchange, 404, Map.of("error", "order not found", "orderId", orderId));
                return;
            }
            writeJson(exchange, 200, state);
        } catch (NumberFormatException e) {
            handleApiFailure(exchange, "get order", new BadRequestException("invalid orderId", e));
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "get order", e);
        }
    }

    private void handleRecentOrders(HttpServerExchange exchange) throws IOException {
        int limit = DEFAULT_RECENT_ORDERS_LIMIT;
        if (exchange.getQueryParameters().containsKey("limit")) {
            try {
                limit = Math.max(1, Integer.parseInt(exchange.getQueryParameters().get("limit").getFirst()));
            } catch (NumberFormatException ignored) {
                limit = DEFAULT_RECENT_ORDERS_LIMIT;
            }
        }
        try {
            writeJson(exchange, 200, Map.of(
                    "items", nodeService.recentOrderStates(limit),
                    "limit", limit
            ));
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "list recent orders", e);
        }
    }

    private void handleCancelOrder(HttpServerExchange exchange) throws IOException {
        CancelOrderRequest request = readCancelOrderRequest(exchange);
        try {
            WriteAdmissionController.Admission admission = writeAdmissionController.acquireForCancel(exchange);
            try (admission) {
                String idempotencyKey = resolveIdempotencyKey(exchange, request.idempotencyKey(), defaultCancelIdempotencyKey(request.orderId()));
                SubmissionReceipt receipt = nodeService.submitTrackedCancelOrder(request.orderId(), idempotencyKey)
                        .awaitLocalReceipt(writeBudget.timeoutMillis());
                writeSubmissionResponse(exchange, receipt);
            }
        } catch (IOException e) {
            handleServiceFailure(exchange, "cancel order", e);
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "cancel order", e);
        }
    }

    private void handleGetSubmission(HttpServerExchange exchange) throws IOException {
        String submissionId = exchange.getPathParameters().containsKey("submissionId")
                ? exchange.getPathParameters().get("submissionId").getFirst()
                : null;
        if (submissionId == null || submissionId.isBlank()) {
            String requestPath = exchange.getRequestPath();
            int slash = requestPath.lastIndexOf('/');
            if (slash >= 0 && slash + 1 < requestPath.length()) {
                submissionId = requestPath.substring(slash + 1);
            }
        }
        if (submissionId == null || submissionId.isBlank()) {
            writeJson(exchange, 400, Map.of("error", "missing submissionId"));
            return;
        }
        SubmissionView view = nodeService.submission(submissionId);
        if (view == null) {
            writeJson(exchange, 404, Map.of("error", "submission not found", "submissionId", submissionId));
            return;
        }
        writeJson(exchange, submissionStatusCode(view), submissionPayload(view));
    }

    private void handleGetSubmissionByKey(HttpServerExchange exchange) throws IOException {
        String idempotencyKey = exchange.getQueryParameters().containsKey("idempotencyKey")
                ? exchange.getQueryParameters().get("idempotencyKey").getFirst()
                : null;
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            writeJson(exchange, 400, Map.of("error", "missing idempotencyKey"));
            return;
        }
        SubmissionView view = nodeService.submissionByIdempotencyKey(idempotencyKey);
        if (view == null) {
            writeJson(exchange, 404, Map.of("error", "submission not found", "idempotencyKey", idempotencyKey));
            return;
        }
        writeJson(exchange, submissionStatusCode(view), submissionPayload(view));
    }

    private void handleCreateSnapshot(HttpServerExchange exchange) throws IOException {
        try {
            var snapshot = nodeService.createSnapshot();
            writeJson(exchange, 200, Map.of(
                    "file", snapshot.file().toString(),
                    "lastSequence", snapshot.lastSequence(),
                    "lastTradeId", snapshot.lastTradeId(),
                    "liveOrderCount", snapshot.liveOrderCount()
            ));
        } catch (IOException e) {
            handleServiceFailure(exchange, "create snapshot", e);
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "create snapshot", e);
        }
    }

    private void handleHealth(HttpServerExchange exchange) throws IOException {
        try {
            NodeControlState state = nodeService.health();
            MatcherNodeMetricsSnapshot metrics = nodeService.metricsSnapshot();
            ClusterSupervisorMetricsSnapshot cluster = clusterMetricsSupplier.get();
            ReadinessSnapshot readiness = readinessSupplier.get();
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("nodeId", state.nodeId());
            payload.put("role", state.role().name());
            payload.put("fencingEpoch", state.fencingToken().epoch());
            payload.put("acceptingClientCommands", state.acceptingClientCommands());
            payload.put("loopState", state.loopState().name());
            payload.put("processedCommandCount", state.processedCommandCount());
            payload.put("tradeCount", metrics.matchingMetrics().tradeCount());
            payload.put("orderEventCount", metrics.matchingMetrics().orderEventCount());
            payload.put("rejectedCommandCount", metrics.matchingMetrics().rejectedCommandCount());
            payload.put("capacityRejectedCommandCount", metrics.matchingMetrics().capacityRejectedCommandCount());
            payload.put("lastReceivedSequence", state.cursor().lastReceivedSequence());
            payload.put("lastDurableSequence", state.cursor().lastDurableSequence());
            payload.put("lastAppliedSequence", state.cursor().lastAppliedSequence());
            payload.put("snapshotSequence", state.cursor().snapshotSequence());
            payload.put("ttlGuardEnabled", metrics.ttlMetrics().enabled());
            payload.put("ttlTrackedOrders", metrics.ttlMetrics().activeTrackedOrders());
            payload.put("ttlRecentAuditEntries", metrics.ttlMetrics().recentAuditEntries());
            payload.put("submitQueueDepth", metrics.submitPathMetrics().submitQueueDepth());
            payload.put("submitQueueCapacity", metrics.submitPathMetrics().submitQueueCapacity());
            payload.put("ringDepth", metrics.submitPathMetrics().ringDepth());
            payload.put("ringRemainingCapacity", metrics.submitPathMetrics().ringRemainingCapacity());
            payload.put("gatewayAcceptedTotal", metrics.submitPathMetrics().walAcceptedTotal());
            payload.put("gatewayWalAppendedTotal", metrics.submitPathMetrics().walAppendedTotal());
            payload.put("gatewayWalForcedTotal", metrics.submitPathMetrics().walForcedTotal());
            payload.put("gatewayFailedBeforeWalTotal", metrics.submitPathMetrics().failedBeforeWalTotal());
            payload.put("gatewayFailedAfterWalTotal", metrics.submitPathMetrics().failedAfterWalTotal());
            payload.put("gatewayLastSubmitResult", metrics.submitPathMetrics().lastSubmitResult());
            payload.put("submissionTrackedCount", metrics.submissionMetrics().trackedCount());
            payload.put("submissionPendingCount", metrics.submissionMetrics().pendingCount());
            payload.put("submissionCommittedCount", metrics.submissionMetrics().committedCount());
            payload.put("submissionFailedCount", metrics.submissionMetrics().failedCount());
            payload.put("submissionRetryingCount", metrics.submissionMetrics().retryingCount());
            payload.put("replicationQueueDepth", metrics.replicationMetrics().queueDepth());
            payload.put("replicationQueueCapacity", metrics.replicationMetrics().queueCapacity());
            payload.put("replicationMaxObservedQueueDepth", metrics.replicationMetrics().maxObservedQueueDepth());
            payload.put("replicationLastBatchSize", metrics.replicationMetrics().lastBatchSize());
            payload.put("replicationMaxObservedBatchSize", metrics.replicationMetrics().maxObservedBatchSize());
            payload.put("replicationBatchesTotal", metrics.replicationMetrics().batchesReplicatedTotal());
            payload.put("replicationCommandsTotal", metrics.replicationMetrics().commandsReplicatedTotal());
            payload.put("replicationCommittedSequence", metrics.replicationMetrics().lastCommittedSequence());
            payload.put("replicationRetryCount", metrics.replicationMetrics().retryCount());
            payload.put("replicationLastAccumulationMicros", metrics.replicationMetrics().lastAccumulationMicros());
            payload.put("replicationLastCommitMicros", metrics.replicationMetrics().lastCommitMicros());
            payload.put("replicationLastBackoffMicros", metrics.replicationMetrics().lastBackoffMicros());
            payload.put("standbyApplyQueueDepth", metrics.standbySyncMetrics().applyQueueDepth());
            payload.put("standbyApplyQueueCapacity", metrics.standbySyncMetrics().applyQueueCapacity());
            payload.put("standbyMaxObservedApplyQueueDepth", metrics.standbySyncMetrics().maxObservedApplyQueueDepth());
            payload.put("standbyLastReplicatedBatchSize", metrics.standbySyncMetrics().lastReplicatedBatchSize());
            payload.put("standbyMaxObservedReplicatedBatchSize", metrics.standbySyncMetrics().maxObservedReplicatedBatchSize());
            payload.put("standbyReplicatedBatchesTotal", metrics.standbySyncMetrics().replicatedBatchesTotal());
            payload.put("standbyReplicatedCommandsTotal", metrics.standbySyncMetrics().replicatedCommandsTotal());
            payload.put("standbyAckFlushCount", metrics.standbySyncMetrics().ackFlushCount());
            payload.put("standbyAckLastFlushCommands", metrics.standbySyncMetrics().lastAckFlushCommands());
            payload.put("standbyAckLastFlushMicros", metrics.standbySyncMetrics().lastAckFlushMicros());
            payload.put("standbyAckLastFlushIntervalMicros", metrics.standbySyncMetrics().lastAckFlushIntervalMicros());
            payload.put("replicationTransport", cluster.transportMetrics().transportType());
            payload.put("transportPolicyStatus", cluster.transportMetrics().policyStatus());
            payload.put("transportPolicyConclusion", cluster.transportMetrics().policyConclusion());
            payload.put("transportReconciliationStatus", cluster.transportMetrics().reconciliationStatus());
            payload.put("transportReconciliationConclusion", cluster.transportMetrics().reconciliationConclusion());
            payload.put("transportPreviewLastReceivedSequence", cluster.transportMetrics().previewLastReceivedSequence());
            payload.put("transportAuthoritativeLastReceivedSequence", cluster.transportMetrics().authoritativeLastReceivedSequence());
            payload.put("transportSnapshotRequests", cluster.transportMetrics().snapshotRequests());
            payload.put("transportSnapshotRequestFailures", cluster.transportMetrics().snapshotRequestFailures());
            payload.put("transportSnapshotBytesSent", cluster.transportMetrics().snapshotBytesSent());
            payload.put("transportSnapshotBytesReceived", cluster.transportMetrics().snapshotBytesReceived());
            payload.put("transportControlRequests", cluster.transportMetrics().controlRequests());
            payload.put("transportControlRequestFailures", cluster.transportMetrics().controlRequestFailures());
            payload.put("transportSecurityGeneration", readiness.transportSecurityGeneration());
            payload.put("transportSecurityReloadCount", readiness.transportSecurityReloadCount());
            payload.put("transportSecurityFailureCount", readiness.transportSecurityFailureCount());
            payload.put("transportSecurityReloadInProgress", readiness.tlsReloadInProgress());
            payload.put("transportSecurityLastError", readiness.transportSecurityLastError());
            writeJson(exchange, 200, payload);
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "runtime health", e);
        }
    }

    private void handleMetrics(HttpServerExchange exchange) throws IOException {
        try {
            NodeControlState state = nodeService.health();
            MatcherNodeMetricsSnapshot nodeMetrics = nodeService.metricsSnapshot();
            GrpcTransportMetrics.Snapshot grpc = grpcMetrics.snapshot();
            ClusterSupervisorMetricsSnapshot cluster = clusterMetricsSupplier.get();
            ReadinessSnapshot readiness = readinessSupplier.get();
            StringBuilder builder = new StringBuilder(METRICS_BUFFER_INITIAL_CAPACITY)
                    .append("# TYPE ull_matcher_grpc_unary_replications_total counter\n")
                    .append("ull_matcher_grpc_unary_replications_total ").append(grpc.unaryReplications()).append('\n')
                    .append("# TYPE ull_matcher_grpc_stream_batches_total counter\n")
                    .append("ull_matcher_grpc_stream_batches_total ").append(grpc.streamedBatches()).append('\n')
                    .append("# TYPE ull_matcher_grpc_stream_commands_total counter\n")
                    .append("ull_matcher_grpc_stream_commands_total ").append(grpc.streamedCommands()).append('\n')
                    .append("# TYPE ull_matcher_grpc_snapshot_bytes_sent_total counter\n")
                    .append("ull_matcher_grpc_snapshot_bytes_sent_total ").append(grpc.snapshotBytesSent()).append('\n')
                    .append("# TYPE ull_matcher_grpc_snapshot_bytes_received_total counter\n")
                    .append("ull_matcher_grpc_snapshot_bytes_received_total ").append(grpc.snapshotBytesReceived()).append('\n')
                    .append("# TYPE ull_matcher_grpc_rejected_ingress_total counter\n")
                    .append("ull_matcher_grpc_rejected_ingress_total ").append(grpc.rejectedIngress()).append('\n')
                    .append("# TYPE ull_matcher_grpc_failures_total counter\n")
                    .append("ull_matcher_grpc_failures_total ").append(grpc.failures()).append('\n')
                    .append("# TYPE ull_matcher_role gauge\n")
                    .append("ull_matcher_role{role=\"").append(state.role().name()).append("\"} 1\n")
                    .append("# TYPE ull_matcher_last_applied_sequence gauge\n")
                    .append("ull_matcher_last_applied_sequence ").append(state.cursor().lastAppliedSequence()).append('\n')
                    .append("# TYPE ull_matcher_snapshot_sequence gauge\n")
                    .append("ull_matcher_snapshot_sequence ").append(state.cursor().snapshotSequence()).append('\n')
                    .append("# TYPE ull_matcher_live_orders gauge\n")
                    .append("ull_matcher_live_orders ").append(nodeMetrics.liveOrderCount()).append('\n')
                    .append("# TYPE ull_matcher_last_trade_id gauge\n")
                    .append("ull_matcher_last_trade_id ").append(nodeMetrics.lastTradeId()).append('\n')
                    .append("# TYPE ull_matcher_loop_processed_commands counter\n")
                    .append("ull_matcher_loop_processed_commands ").append(nodeMetrics.loopSnapshot().processedCommandCount()).append('\n')
                    .append("# TYPE ull_matcher_trade_events_total counter\n")
                    .append("ull_matcher_trade_events_total ").append(nodeMetrics.matchingMetrics().tradeCount()).append('\n')
                    .append("# TYPE ull_matcher_order_events_total counter\n")
                    .append("ull_matcher_order_events_total ").append(nodeMetrics.matchingMetrics().orderEventCount()).append('\n')
                    .append("# TYPE ull_matcher_rejected_commands_total counter\n")
                    .append("ull_matcher_rejected_commands_total ").append(nodeMetrics.matchingMetrics().rejectedCommandCount()).append('\n')
                    .append("# TYPE ull_matcher_capacity_rejected_commands_total counter\n")
                    .append("ull_matcher_capacity_rejected_commands_total ").append(nodeMetrics.matchingMetrics().capacityRejectedCommandCount()).append('\n')
                    .append("# TYPE ull_matcher_wal_segments gauge\n")
                    .append("ull_matcher_wal_segments ").append(nodeMetrics.walSegmentCount()).append('\n')
                    .append("# TYPE ull_matcher_wal_current_segment_bytes gauge\n")
                    .append("ull_matcher_wal_current_segment_bytes ").append(nodeMetrics.currentWalSegmentBytes()).append('\n')
                    .append("# TYPE ull_matcher_submit_queue_depth gauge\n")
                    .append("ull_matcher_submit_queue_depth ").append(nodeMetrics.submitPathMetrics().submitQueueDepth()).append('\n')
                    .append("# TYPE ull_matcher_submit_queue_capacity gauge\n")
                    .append("ull_matcher_submit_queue_capacity ").append(nodeMetrics.submitPathMetrics().submitQueueCapacity()).append('\n')
                    .append("# TYPE ull_matcher_ring_depth gauge\n")
                    .append("ull_matcher_ring_depth ").append(nodeMetrics.submitPathMetrics().ringDepth()).append('\n')
                    .append("# TYPE ull_matcher_ring_remaining_capacity gauge\n")
                    .append("ull_matcher_ring_remaining_capacity ").append(nodeMetrics.submitPathMetrics().ringRemainingCapacity()).append('\n')
                    .append("# TYPE ull_matcher_gateway_accepted_total counter\n")
                    .append("ull_matcher_gateway_accepted_total ").append(nodeMetrics.submitPathMetrics().walAcceptedTotal()).append('\n')
                    .append("# TYPE ull_matcher_gateway_wal_appended_total counter\n")
                    .append("ull_matcher_gateway_wal_appended_total ").append(nodeMetrics.submitPathMetrics().walAppendedTotal()).append('\n')
                    .append("# TYPE ull_matcher_gateway_wal_forced_total counter\n")
                    .append("ull_matcher_gateway_wal_forced_total ").append(nodeMetrics.submitPathMetrics().walForcedTotal()).append('\n')
                    .append("# TYPE ull_matcher_gateway_failed_before_wal_total counter\n")
                    .append("ull_matcher_gateway_failed_before_wal_total ").append(nodeMetrics.submitPathMetrics().failedBeforeWalTotal()).append('\n')
                    .append("# TYPE ull_matcher_gateway_failed_after_wal_total counter\n")
                    .append("ull_matcher_gateway_failed_after_wal_total ").append(nodeMetrics.submitPathMetrics().failedAfterWalTotal()).append('\n')
                    .append("# TYPE ull_matcher_submission_tracked gauge\n")
                    .append("ull_matcher_submission_tracked ").append(nodeMetrics.submissionMetrics().trackedCount()).append('\n')
                    .append("# TYPE ull_matcher_submission_pending gauge\n")
                    .append("ull_matcher_submission_pending ").append(nodeMetrics.submissionMetrics().pendingCount()).append('\n')
                    .append("# TYPE ull_matcher_submission_committed gauge\n")
                    .append("ull_matcher_submission_committed ").append(nodeMetrics.submissionMetrics().committedCount()).append('\n')
                    .append("# TYPE ull_matcher_submission_failed gauge\n")
                    .append("ull_matcher_submission_failed ").append(nodeMetrics.submissionMetrics().failedCount()).append('\n')
                    .append("# TYPE ull_matcher_submission_retrying gauge\n")
                    .append("ull_matcher_submission_retrying ").append(nodeMetrics.submissionMetrics().retryingCount()).append('\n')
                    .append("# TYPE ull_matcher_replication_queue_depth gauge\n")
                    .append("ull_matcher_replication_queue_depth ").append(nodeMetrics.replicationMetrics().queueDepth()).append('\n')
                    .append("# TYPE ull_matcher_replication_queue_capacity gauge\n")
                    .append("ull_matcher_replication_queue_capacity ").append(nodeMetrics.replicationMetrics().queueCapacity()).append('\n')
                    .append("# TYPE ull_matcher_replication_queue_high_watermark gauge\n")
                    .append("ull_matcher_replication_queue_high_watermark ").append(nodeMetrics.replicationMetrics().maxObservedQueueDepth()).append('\n')
                    .append("# TYPE ull_matcher_replication_last_batch_size gauge\n")
                    .append("ull_matcher_replication_last_batch_size ").append(nodeMetrics.replicationMetrics().lastBatchSize()).append('\n')
                    .append("# TYPE ull_matcher_replication_batch_high_watermark gauge\n")
                    .append("ull_matcher_replication_batch_high_watermark ").append(nodeMetrics.replicationMetrics().maxObservedBatchSize()).append('\n')
                    .append("# TYPE ull_matcher_replication_batches_total counter\n")
                    .append("ull_matcher_replication_batches_total ").append(nodeMetrics.replicationMetrics().batchesReplicatedTotal()).append('\n')
                    .append("# TYPE ull_matcher_replication_commands_total counter\n")
                    .append("ull_matcher_replication_commands_total ").append(nodeMetrics.replicationMetrics().commandsReplicatedTotal()).append('\n')
                    .append("# TYPE ull_matcher_replication_committed_sequence gauge\n")
                    .append("ull_matcher_replication_committed_sequence ").append(nodeMetrics.replicationMetrics().lastCommittedSequence()).append('\n')
                    .append("# TYPE ull_matcher_replication_retries_total counter\n")
                    .append("ull_matcher_replication_retries_total ").append(nodeMetrics.replicationMetrics().retryCount()).append('\n')
                    .append("# TYPE ull_matcher_replication_last_accumulation_micros gauge\n")
                    .append("ull_matcher_replication_last_accumulation_micros ").append(nodeMetrics.replicationMetrics().lastAccumulationMicros()).append('\n')
                    .append("# TYPE ull_matcher_replication_last_commit_micros gauge\n")
                    .append("ull_matcher_replication_last_commit_micros ").append(nodeMetrics.replicationMetrics().lastCommitMicros()).append('\n')
                    .append("# TYPE ull_matcher_replication_last_backoff_micros gauge\n")
                    .append("ull_matcher_replication_last_backoff_micros ").append(nodeMetrics.replicationMetrics().lastBackoffMicros()).append('\n')
                    .append("# TYPE ull_matcher_standby_apply_queue_depth gauge\n")
                    .append("ull_matcher_standby_apply_queue_depth ").append(nodeMetrics.standbySyncMetrics().applyQueueDepth()).append('\n')
                    .append("# TYPE ull_matcher_standby_apply_queue_capacity gauge\n")
                    .append("ull_matcher_standby_apply_queue_capacity ").append(nodeMetrics.standbySyncMetrics().applyQueueCapacity()).append('\n')
                    .append("# TYPE ull_matcher_standby_apply_queue_high_watermark gauge\n")
                    .append("ull_matcher_standby_apply_queue_high_watermark ").append(nodeMetrics.standbySyncMetrics().maxObservedApplyQueueDepth()).append('\n')
                    .append("# TYPE ull_matcher_standby_last_replicated_batch_size gauge\n")
                    .append("ull_matcher_standby_last_replicated_batch_size ").append(nodeMetrics.standbySyncMetrics().lastReplicatedBatchSize()).append('\n')
                    .append("# TYPE ull_matcher_standby_replicated_batch_high_watermark gauge\n")
                    .append("ull_matcher_standby_replicated_batch_high_watermark ").append(nodeMetrics.standbySyncMetrics().maxObservedReplicatedBatchSize()).append('\n')
                    .append("# TYPE ull_matcher_standby_replicated_batches_total counter\n")
                    .append("ull_matcher_standby_replicated_batches_total ").append(nodeMetrics.standbySyncMetrics().replicatedBatchesTotal()).append('\n')
                    .append("# TYPE ull_matcher_standby_replicated_commands_total counter\n")
                    .append("ull_matcher_standby_replicated_commands_total ").append(nodeMetrics.standbySyncMetrics().replicatedCommandsTotal()).append('\n')
                    .append("# TYPE ull_matcher_standby_ack_flush_total counter\n")
                    .append("ull_matcher_standby_ack_flush_total ").append(nodeMetrics.standbySyncMetrics().ackFlushCount()).append('\n')
                    .append("# TYPE ull_matcher_standby_last_ack_flush_commands gauge\n")
                    .append("ull_matcher_standby_last_ack_flush_commands ").append(nodeMetrics.standbySyncMetrics().lastAckFlushCommands()).append('\n')
                    .append("# TYPE ull_matcher_standby_last_ack_flush_micros gauge\n")
                    .append("ull_matcher_standby_last_ack_flush_micros ").append(nodeMetrics.standbySyncMetrics().lastAckFlushMicros()).append('\n')
                    .append("# TYPE ull_matcher_standby_last_ack_flush_interval_micros gauge\n")
                    .append("ull_matcher_standby_last_ack_flush_interval_micros ").append(nodeMetrics.standbySyncMetrics().lastAckFlushIntervalMicros()).append('\n')
                    .append("# TYPE ull_matcher_readiness_service_ready gauge\n")
                    .append("ull_matcher_readiness_service_ready ").append(readiness.serviceReady() ? 1 : 0).append('\n')
                    .append("# TYPE ull_matcher_readiness_client_traffic_ready gauge\n")
                    .append("ull_matcher_readiness_client_traffic_ready ").append(readiness.clientTrafficReady() ? 1 : 0).append('\n')
                    .append("# TYPE ull_matcher_transport_security_generation gauge\n")
                    .append("ull_matcher_transport_security_generation ").append(readiness.transportSecurityGeneration()).append('\n')
                    .append("# TYPE ull_matcher_transport_security_reload_count_total counter\n")
                    .append("ull_matcher_transport_security_reload_count_total ").append(readiness.transportSecurityReloadCount()).append('\n')
                    .append("# TYPE ull_matcher_transport_security_reload_failures_total counter\n")
                    .append("ull_matcher_transport_security_reload_failures_total ").append(readiness.transportSecurityFailureCount()).append('\n')
                    .append("# TYPE ull_matcher_transport_security_reloading gauge\n")
                    .append("ull_matcher_transport_security_reloading ").append(readiness.tlsReloadInProgress() ? 1 : 0).append('\n');
            if (cluster.lastGateDecision() != null) {
                builder.append("# TYPE ull_matcher_ha_promotion_ready gauge\n")
                        .append("ull_matcher_ha_promotion_ready ").append(cluster.lastGateDecision().promotionReady() ? 1 : 0).append('\n')
                        .append("# TYPE ull_matcher_ha_snapshot_sync_required gauge\n")
                        .append("ull_matcher_ha_snapshot_sync_required ").append(cluster.lastGateDecision().snapshotSyncRequired() ? 1 : 0).append('\n')
                        .append("# TYPE ull_matcher_ha_received_lag gauge\n")
                        .append("ull_matcher_ha_received_lag ").append(cluster.lastGateDecision().report().receivedLag()).append('\n')
                        .append("# TYPE ull_matcher_ha_durable_lag gauge\n")
                        .append("ull_matcher_ha_durable_lag ").append(cluster.lastGateDecision().report().durableLag()).append('\n')
                        .append("# TYPE ull_matcher_ha_applied_lag gauge\n")
                        .append("ull_matcher_ha_applied_lag ").append(cluster.lastGateDecision().report().appliedLag()).append('\n')
                        .append("# TYPE ull_matcher_ha_snapshot_lag gauge\n")
                        .append("ull_matcher_ha_snapshot_lag ").append(cluster.lastGateDecision().report().snapshotLag()).append('\n');
            }
            builder.append("# TYPE ull_matcher_ha_tick_total counter\n")
                    .append("ull_matcher_ha_tick_total ").append(cluster.tickCount()).append('\n')
                    .append("# TYPE ull_matcher_ha_tick_failures_total counter\n")
                    .append("ull_matcher_ha_tick_failures_total ").append(cluster.tickFailureCount()).append('\n')
                    .append("# TYPE ull_matcher_ha_replication_transport gauge\n")
                    .append("ull_matcher_ha_replication_transport{transport=\"")
                    .append(escapeLabelValue(cluster.transportMetrics().transportType()))
                    .append("\"} 1\n")
                    .append("# TYPE ull_matcher_ha_transport_preview_published_total counter\n")
                    .append("ull_matcher_ha_transport_preview_published_total ")
                    .append(cluster.transportMetrics().previewPublishedCommands()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_preview_published_bytes_total counter\n")
                    .append("ull_matcher_ha_transport_preview_published_bytes_total ")
                    .append(cluster.transportMetrics().previewPublishedBytes()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_preview_publish_failures_total counter\n")
                    .append("ull_matcher_ha_transport_preview_publish_failures_total ")
                    .append(cluster.transportMetrics().previewPublishFailures()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_preview_received_total counter\n")
                    .append("ull_matcher_ha_transport_preview_received_total ")
                    .append(cluster.transportMetrics().previewReceivedCommands()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_preview_received_bytes_total counter\n")
                    .append("ull_matcher_ha_transport_preview_received_bytes_total ")
                    .append(cluster.transportMetrics().previewReceivedBytes()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_snapshot_requests_total counter\n")
                    .append("ull_matcher_ha_transport_snapshot_requests_total ")
                    .append(cluster.transportMetrics().snapshotRequests()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_snapshot_request_failures_total counter\n")
                    .append("ull_matcher_ha_transport_snapshot_request_failures_total ")
                    .append(cluster.transportMetrics().snapshotRequestFailures()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_snapshot_bytes_sent_total counter\n")
                    .append("ull_matcher_ha_transport_snapshot_bytes_sent_total ")
                    .append(cluster.transportMetrics().snapshotBytesSent()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_snapshot_bytes_received_total counter\n")
                    .append("ull_matcher_ha_transport_snapshot_bytes_received_total ")
                    .append(cluster.transportMetrics().snapshotBytesReceived()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_control_requests_total counter\n")
                    .append("ull_matcher_ha_transport_control_requests_total ")
                    .append(cluster.transportMetrics().controlRequests()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_control_request_failures_total counter\n")
                    .append("ull_matcher_ha_transport_control_request_failures_total ")
                    .append(cluster.transportMetrics().controlRequestFailures()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_authoritative_last_received_sequence gauge\n")
                    .append("ull_matcher_ha_transport_authoritative_last_received_sequence ")
                    .append(cluster.transportMetrics().authoritativeLastReceivedSequence()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_preview_last_received_sequence gauge\n")
                    .append("ull_matcher_ha_transport_preview_last_received_sequence ")
                    .append(cluster.transportMetrics().previewLastReceivedSequence()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_preview_gap_total counter\n")
                    .append("ull_matcher_ha_transport_preview_gap_total ")
                    .append(cluster.transportMetrics().previewGapCount()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_preview_out_of_order_total counter\n")
                    .append("ull_matcher_ha_transport_preview_out_of_order_total ")
                    .append(cluster.transportMetrics().previewOutOfOrderCount()).append('\n')
                    .append("# TYPE ull_matcher_ha_transport_reconciliation_status gauge\n")
                    .append("ull_matcher_ha_transport_reconciliation_status{status=\"")
                    .append(escapeLabelValue(cluster.transportMetrics().reconciliationStatus()))
                    .append("\"} 1\n")
                    .append("# TYPE ull_matcher_ha_transport_policy_status gauge\n")
                    .append("ull_matcher_ha_transport_policy_status{status=\"")
                    .append(escapeLabelValue(cluster.transportMetrics().policyStatus()))
                    .append("\"} 1\n");
            cluster.errorCounts().forEach((category, count) -> builder
                    .append("ull_matcher_ha_error_total{category=\"").append(category).append("\"} ").append(count).append('\n'));
            int globalInflight = maxConcurrentRequests - requestSlots.availablePermits();
            double globalSaturation = ((double) globalInflight) / maxConcurrentRequests;
            int executorQueueDepth = requestExecutor.getQueue().size();
            builder.append("# TYPE ull_matcher_http_global_overload_total counter\n")
                    .append("ull_matcher_http_global_overload_total ").append(globalOverloadCount.get()).append('\n')
                    .append("# TYPE ull_matcher_http_global_inflight gauge\n")
                    .append("ull_matcher_http_global_inflight ").append(globalInflight).append('\n')
                    .append("# TYPE ull_matcher_http_global_saturation gauge\n")
                    .append("ull_matcher_http_global_saturation ").append(globalSaturation).append('\n')
                    .append("# TYPE ull_matcher_http_executor_queue_depth gauge\n")
                    .append("ull_matcher_http_executor_queue_depth ").append(executorQueueDepth).append('\n')
                    .append("# TYPE ull_matcher_http_executor_queue_capacity gauge\n")
                    .append("ull_matcher_http_executor_queue_capacity ").append(requestExecutorQueueCapacity).append('\n');
            builder.append("# TYPE ull_matcher_http_shard_write_overload_total counter\n")
                    .append("ull_matcher_http_shard_write_overload_total{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                    .append(writeAdmissionController.shardOverloadCount()).append('\n')
                    .append("# TYPE ull_matcher_http_shard_write_inflight gauge\n")
                    .append("ull_matcher_http_shard_write_inflight{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                    .append(writeAdmissionController.shardInflight()).append('\n')
                    .append("# TYPE ull_matcher_http_shard_write_saturation gauge\n")
                    .append("ull_matcher_http_shard_write_saturation{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                    .append(writeAdmissionController.shardSaturation()).append('\n')
                    .append("# TYPE ull_matcher_http_shard_write_rate_limited_total counter\n")
                    .append("ull_matcher_http_shard_write_rate_limited_total{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                    .append(writeAdmissionController.shardRateLimitedCount()).append('\n')
                    .append("# TYPE ull_matcher_http_shard_write_rate_limit_per_second gauge\n")
                    .append("ull_matcher_http_shard_write_rate_limit_per_second{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                    .append(writeAdmissionController.shardRateLimitPerSecond()).append('\n')
                    .append("# TYPE ull_matcher_http_tenant_write_overload_total counter\n")
                    .append("ull_matcher_http_tenant_write_overload_total ").append(writeAdmissionController.tenantOverloadCount()).append('\n')
                    .append("# TYPE ull_matcher_http_tenant_write_rate_limited_total counter\n")
                    .append("ull_matcher_http_tenant_write_rate_limited_total ").append(writeAdmissionController.tenantRateLimitedCount()).append('\n')
                    .append("# TYPE ull_matcher_http_tenant_write_rate_limit_per_second gauge\n")
                    .append("ull_matcher_http_tenant_write_rate_limit_per_second ").append(writeAdmissionController.tenantRateLimitPerSecond()).append('\n')
                    .append("# TYPE ull_matcher_http_tenant_write_default_weight gauge\n")
                    .append("ull_matcher_http_tenant_write_default_weight ").append(writeAdmissionController.tenantDefaultWeight()).append('\n')
                    .append("# TYPE ull_matcher_http_tenant_write_active_entries gauge\n")
                    .append("ull_matcher_http_tenant_write_active_entries ").append(writeAdmissionController.activeTenantBudgets()).append('\n')
                    .append("# TYPE ull_matcher_http_tenant_write_anonymous_total counter\n")
                    .append("ull_matcher_http_tenant_write_anonymous_total ").append(writeAdmissionController.anonymousTenantRequests()).append('\n');
            builder.append("# TYPE ull_matcher_http_route_requests_total counter\n")
                    .append("# TYPE ull_matcher_http_route_overload_total counter\n")
                    .append("# TYPE ull_matcher_http_route_timeout_total counter\n")
                    .append("# TYPE ull_matcher_http_route_inflight gauge\n")
                    .append("# TYPE ull_matcher_http_route_saturation gauge\n");
            appendRouteMetrics(builder, writeBudget);
            appendRouteMetrics(builder, readBudget);
            appendRouteMetrics(builder, adminBudget);
            builder.append("# TYPE ull_matcher_http_endpoint_requests_total counter\n")
                    .append("# TYPE ull_matcher_http_endpoint_overload_total counter\n")
                    .append("# TYPE ull_matcher_http_endpoint_timeouts_total counter\n")
                    .append("# TYPE ull_matcher_http_endpoint_failures_total counter\n")
                    .append("# TYPE ull_matcher_http_endpoint_inflight gauge\n")
                    .append("# TYPE ull_matcher_http_endpoint_max_inflight gauge\n")
                    .append("# TYPE ull_matcher_http_endpoint_budget_saturation gauge\n")
                    .append("# TYPE ull_matcher_http_endpoint_route_saturation_share gauge\n")
                    .append("# TYPE ull_matcher_http_endpoint_duration_ms_sum counter\n")
                    .append("# TYPE ull_matcher_http_endpoint_duration_ms_max gauge\n")
                    .append("# TYPE ull_matcher_http_endpoint_duration_bucket counter\n");
            endpointStats.forEach((endpoint, stats) -> appendEndpointMetrics(builder, endpoint, stats));
            builder.append("# TYPE ull_matcher_ttl_active_tracked_orders gauge\n")
                    .append("ull_matcher_ttl_active_tracked_orders ").append(nodeMetrics.ttlMetrics().activeTrackedOrders()).append('\n')
                    .append("# TYPE ull_matcher_ttl_pending_submissions gauge\n")
                    .append("ull_matcher_ttl_pending_submissions ").append(nodeMetrics.ttlMetrics().pendingSubmissions()).append('\n')
                    .append("# TYPE ull_matcher_ttl_scheduled_total counter\n")
                    .append("ull_matcher_ttl_scheduled_total ").append(nodeMetrics.ttlMetrics().scheduledTotal()).append('\n')
                    .append("# TYPE ull_matcher_ttl_cancel_requested_total counter\n")
                    .append("ull_matcher_ttl_cancel_requested_total ").append(nodeMetrics.ttlMetrics().cancelRequestedTotal()).append('\n')
                    .append("# TYPE ull_matcher_ttl_cancel_accepted_total counter\n")
                    .append("ull_matcher_ttl_cancel_accepted_total ").append(nodeMetrics.ttlMetrics().cancelAcceptedTotal()).append('\n')
                    .append("# TYPE ull_matcher_ttl_cancel_skipped_total counter\n")
                    .append("ull_matcher_ttl_cancel_skipped_total ").append(nodeMetrics.ttlMetrics().cancelSkippedTotal()).append('\n')
                    .append("# TYPE ull_matcher_ttl_cancel_failed_total counter\n")
                    .append("ull_matcher_ttl_cancel_failed_total ").append(nodeMetrics.ttlMetrics().cancelFailedTotal()).append('\n');
            byte[] bytes = builder.toString().getBytes(StandardCharsets.UTF_8);
            exchange.setStatusCode(200);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, METRICS_CONTENT_TYPE);
            exchange.getResponseSender().send(java.nio.ByteBuffer.wrap(bytes));
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "metrics", e);
        }
    }

    private void handleReadiness(HttpServerExchange exchange) throws IOException {
        try {
            ReadinessSnapshot readiness = readinessSupplier.get();
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("serviceReady", readiness.serviceReady());
            payload.put("clientTrafficReady", readiness.clientTrafficReady());
            payload.put("promotionReady", readiness.promotionReady());
            payload.put("snapshotSyncRequired", readiness.snapshotSyncRequired());
            payload.put("catchUpInProgress", readiness.catchUpInProgress());
            payload.put("tlsReloadInProgress", readiness.tlsReloadInProgress());
            payload.put("transportSecurityGeneration", readiness.transportSecurityGeneration());
            payload.put("transportSecurityReloadCount", readiness.transportSecurityReloadCount());
            payload.put("transportSecurityFailureCount", readiness.transportSecurityFailureCount());
            payload.put("transportSecurityLastError", readiness.transportSecurityLastError());
            payload.put("syncState", readiness.syncState());
            payload.put("recentErrors", readiness.recentErrors());
            payload.put("lastTickResult", readiness.lastTickResult());
            payload.put("lastGateDecision", readiness.lastGateDecision());
            payload.put("lastTickAction", readiness.lastTickAction());
            payload.put("lastTickReason", readiness.lastTickReason());
            payload.put("lastGateReason", readiness.lastGateReason());
            payload.put("lastGateReceivedLag", readiness.lastGateReceivedLag());
            payload.put("lastGateDurableLag", readiness.lastGateDurableLag());
            payload.put("lastGateAppliedLag", readiness.lastGateAppliedLag());
            payload.put("lastGateSnapshotLag", readiness.lastGateSnapshotLag());
            payload.put("transportPolicyStatus", readiness.transportPolicyStatus());
            payload.put("transportPolicyConclusion", readiness.transportPolicyConclusion());
            payload.put("transportReconciliationStatus", readiness.transportReconciliationStatus());
            payload.put("transportReconciliationConclusion", readiness.transportReconciliationConclusion());
            payload.put("reason", readiness.reason());
            writeJson(exchange, readiness.serviceReady() ? 200 : 503, payload);
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "runtime readiness", e);
        }
    }

    private void handleNotFound(HttpServerExchange exchange) throws IOException {
        writeJson(exchange, 404, Map.of("error", "not found"));
    }

    private void handleServiceFailure(HttpServerExchange exchange, String operation, IOException error) throws IOException {
        ServerApiException apiError = new ServiceUnavailableException(operation + " failed", error);
        logApiFailure(apiError, error);
        writeJson(exchange, apiError.statusCode(), Map.of(
                "error", apiError.getMessage(),
                "code", apiError.errorCode(),
                "detail", error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage()
        ));
    }

    private void handleApiFailure(HttpServerExchange exchange, String operation, RuntimeException error) throws IOException {
        ServerApiException apiError = HttpApiExceptionMapper.map(operation, error);
        logApiFailure(apiError, error);
        writeJson(exchange, apiError.statusCode(), Map.of("error", apiError.getMessage(), "code", apiError.errorCode()));
    }

    private void logApiFailure(ServerApiException apiError, Throwable error) {
        if (apiError.logLevel() == Level.ERROR) {
            LOG.error(apiError.getMessage(), error);
        } else if (apiError.logLevel() == Level.WARN) {
            LOG.warn("{}: {}", apiError.getMessage(), error.getMessage());
        } else if (apiError.logLevel() == Level.INFO) {
            LOG.info(apiError.getMessage());
        } else {
            LOG.debug(apiError.getMessage());
        }
    }

    private int statusCode(SubmitResult result) {
        return switch (result) {
            case ACCEPTED -> 202;
            case MATCHER_NOT_RUNNING, RING_FULL_BEFORE_WAL_APPEND, COMMAND_POOL_EXHAUSTED, MATCHER_STOPPED_AFTER_WAL_APPEND, RING_FULL_AFTER_WAL_APPEND -> 503;
        };
    }

    private int submissionStatusCode(SubmissionView view) {
        if (!view.localDurable() && view.phase() == SubmissionPhase.FAILED) {
            return statusCode(view.localResult());
        }
        return view.replicationCommitted() ? 200 : 202;
    }

    private int submissionStatusCode(SubmissionReceipt receipt) {
        if (!receipt.localDurable() && receipt.phase() == SubmissionPhase.FAILED) {
            return statusCode(receipt.localResult());
        }
        return receipt.replicationCommitted() ? 200 : 202;
    }

    private void writeSubmissionResponse(HttpServerExchange exchange, SubmissionReceipt receipt) throws IOException {
        exchange.getResponseHeaders().put(Headers.LOCATION, ROUTE_SUBMISSION.replace("{submissionId}", receipt.submissionId()));
        writeJson(exchange, submissionStatusCode(receipt), submissionReceiptPayload(receipt));
    }

    private Map<String, Object> submissionPayload(SubmissionView view) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("submissionId", view.submissionId());
        payload.put("idempotencyKey", view.idempotencyKey());
        payload.put("operationType", view.operationType());
        payload.put("userId", view.userId());
        payload.put("orderId", view.orderId());
        payload.put("sequence", view.sequence());
        payload.put("phase", view.phase().name());
        payload.put("localResult", view.localResult() == null ? null : view.localResult().name());
        payload.put("result", view.localResult() == null ? null : view.localResult().name());
        payload.put("localDurable", view.localDurable());
        payload.put("replicationRequired", view.replicationRequired());
        payload.put("replicationCommitted", view.replicationCommitted());
        payload.put("totalTargets", view.totalTargets());
        payload.put("requiredAcks", view.requiredAcks());
        payload.put("ackedTargets", view.ackedTargets());
        payload.put("ackedNodeIds", view.ackedNodeIds());
        payload.put("failedNodeIds", view.failedNodeIds());
        payload.put("retryCount", view.retryCount());
        payload.put("lastError", view.lastError());
        payload.put("createdAtEpochMillis", view.createdAtEpochMillis());
        payload.put("updatedAtEpochMillis", view.updatedAtEpochMillis());
        payload.put("queryPath", ROUTE_SUBMISSION.replace("{submissionId}", view.submissionId()));
        payload.put("queryByIdempotencyPath", ROUTE_SUBMISSION_BY_KEY + "?idempotencyKey=" + java.net.URLEncoder.encode(view.idempotencyKey(), StandardCharsets.UTF_8));
        payload.put("orderState", view.orderState());
        return payload;
    }

    private Map<String, Object> submissionReceiptPayload(SubmissionReceipt receipt) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("submissionId", receipt.submissionId());
        payload.put("idempotencyKey", receipt.idempotencyKey());
        payload.put("operationType", receipt.operationType());
        payload.put("userId", receipt.userId());
        payload.put("orderId", receipt.orderId());
        payload.put("sequence", receipt.sequence());
        payload.put("phase", receipt.phase().name());
        payload.put("localResult", receipt.localResult() == null ? null : receipt.localResult().name());
        payload.put("result", receipt.localResult() == null ? null : receipt.localResult().name());
        payload.put("localDurable", receipt.localDurable());
        payload.put("replicationRequired", receipt.replicationRequired());
        payload.put("replicationCommitted", receipt.replicationCommitted());
        payload.put("totalTargets", receipt.totalTargets());
        payload.put("requiredAcks", receipt.requiredAcks());
        payload.put("ackedTargets", receipt.ackedTargets());
        payload.put("retryCount", receipt.retryCount());
        payload.put("lastError", receipt.lastError());
        payload.put("createdAtEpochMillis", receipt.createdAtEpochMillis());
        payload.put("updatedAtEpochMillis", receipt.updatedAtEpochMillis());
        payload.put("queryPath", ROUTE_SUBMISSION.replace("{submissionId}", receipt.submissionId()));
        payload.put("queryByIdempotencyPath", ROUTE_SUBMISSION_BY_KEY + "?idempotencyKey=" + java.net.URLEncoder.encode(receipt.idempotencyKey(), StandardCharsets.UTF_8));
        return payload;
    }

    private String resolveIdempotencyKey(HttpServerExchange exchange, String bodyKey, String fallbackKey) {
        String headerKey = exchange.getRequestHeaders().getFirst("Idempotency-Key");
        if (headerKey != null && !headerKey.isBlank()) {
            return headerKey.trim();
        }
        if (bodyKey != null && !bodyKey.isBlank()) {
            return bodyKey.trim();
        }
        return fallbackKey;
    }

    private static String defaultOrderIdempotencyKey(long userId, long orderId) {
        return "new-order:" + userId + ":" + orderId;
    }

    private static String defaultCancelIdempotencyKey(long orderId) {
        return "cancel-order:" + orderId;
    }

    private NewOrderRequest readNewOrderRequest(HttpServerExchange exchange) throws IOException {
        return readRequest(exchange, newOrderRequestReader);
    }

    private CancelOrderRequest readCancelOrderRequest(HttpServerExchange exchange) throws IOException {
        return readRequest(exchange, cancelOrderRequestReader);
    }

    private <T> T readRequest(HttpServerExchange exchange, ObjectReader reader) throws IOException {
        exchange.startBlocking();
        long declaredLength = exchange.getRequestContentLength();
        if (declaredLength > maxBodyBytes) {
            throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
        }
        try (InputStream body = new LimitedBodyInputStream(exchange.getInputStream(), maxBodyBytes)) {
            return reader.readValue(body);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new BadRequestException("invalid request body", e);
        }
    }

    private <T extends Enum<T>> T parseEnum(String raw, Class<T> type, String fieldName) {
        if (raw == null || raw.isBlank()) {
            throw new BadRequestException("missing " + fieldName);
        }
        try {
            return Enum.valueOf(type, raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("invalid " + fieldName + ": " + raw, e);
        }
    }

    private void writeJson(HttpServerExchange exchange, int status, Object body) throws IOException {
        if (!responseGuard(exchange).tryCommit()) {
            return;
        }
        byte[] bytes = objectMapper.writeValueAsBytes(body);
        exchange.setStatusCode(status);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, JSON_CONTENT_TYPE);
        exchange.getResponseSender().send(java.nio.ByteBuffer.wrap(bytes));
    }

    private HttpHandler directBlocking(String endpointMetricKey,
                                       String operation,
                                       RouteBudget routeBudget,
                                       EndpointBudget endpointBudget,
                                       BlockingExchangeHandler handler) {
        return exchange -> {
            EndpointStats endpoint = endpointStats.computeIfAbsent(
                    endpointMetricKey,
                    ignored -> new EndpointStats(
                            endpointMetricKey,
                            routeBudget.name(),
                            routeBudget.maxConcurrentRequests(),
                            endpointBudget == null ? 0 : endpointBudget.maxConcurrentRequests(),
                            ENDPOINT_LATENCY_BUCKETS_MILLIS.length)
            );
            if (!tryAcquireBudgets(exchange, operation, routeBudget, endpointBudget, endpoint, false)) {
                return;
            }
            exchange.dispatch(() -> {
                responseGuard(exchange);
                long startedAt = System.nanoTime();
                long endpointInflight = endpoint.inflight().incrementAndGet();
                endpoint.maxInflight().accumulateAndGet(endpointInflight, Math::max);
                try {
                    routeBudget.requestCount().incrementAndGet();
                    endpoint.requestCount().incrementAndGet();
                    handler.handle(exchange);
                } catch (IOException e) {
                    endpoint.failureCount().incrementAndGet();
                    writeBestEffort(exchange, e);
                } catch (RuntimeException e) {
                    endpoint.failureCount().incrementAndGet();
                    writeBestEffort(exchange, e);
                } finally {
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                    recordEndpointLatency(endpoint, elapsedMillis);
                    endpoint.inflight().decrementAndGet();
                    releaseBudgets(routeBudget, endpointBudget);
                }
            });
        };
    }

    private HttpHandler blocking(String endpointMetricKey, String operation, RouteBudget routeBudget, EndpointBudget endpointBudget, BlockingExchangeHandler handler) {
        return exchange -> {
            EndpointStats endpoint = endpointStats.computeIfAbsent(
                    endpointMetricKey,
                    ignored -> new EndpointStats(
                            endpointMetricKey,
                            routeBudget.name(),
                            routeBudget.maxConcurrentRequests(),
                            endpointBudget == null ? 0 : endpointBudget.maxConcurrentRequests(),
                            ENDPOINT_LATENCY_BUCKETS_MILLIS.length)
            );
            if (!tryAcquireBudgets(exchange, operation, routeBudget, endpointBudget, endpoint, true)) {
                return;
            }
            exchange.dispatch(() -> {
                Future<?> future = null;
                responseGuard(exchange);
                long startedAt = System.nanoTime();
                long endpointInflight = endpoint.inflight().incrementAndGet();
                endpoint.maxInflight().accumulateAndGet(endpointInflight, Math::max);
                try {
                    routeBudget.requestCount().incrementAndGet();
                    endpoint.requestCount().incrementAndGet();
                    future = requestExecutor.submit(() -> {
                        try {
                            handler.handle(exchange);
                        } catch (IOException e) {
                            throw new WrappedIOException(e);
                        }
                    });
                    future.get(routeBudget.timeoutMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    if (future != null) {
                        future.cancel(true);
                    }
                    routeBudget.timeoutCount().incrementAndGet();
                    endpoint.timeoutCount().incrementAndGet();
                    if (!exchange.isResponseStarted()) {
                        writeBestEffort(exchange, new RequestTimeoutException(
                                operation + " exceeded timeout " + routeBudget.timeoutMillis() + "ms"
                        ));
                    }
                } catch (RejectedExecutionException e) {
                    if (!exchange.isResponseStarted()) {
                        writeBestEffort(exchange, new OverloadedException("request executor is not accepting work"));
                    }
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    endpoint.failureCount().incrementAndGet();
                    if (cause instanceof WrappedIOException wrapped) {
                        writeBestEffort(exchange, wrapped.cause);
                    } else if (cause instanceof RuntimeException runtime) {
                        writeBestEffort(exchange, runtime);
                    } else if (!exchange.isResponseStarted()) {
                        writeBestEffort(exchange, new InternalServerException("http handler failed", cause));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    endpoint.failureCount().incrementAndGet();
                    if (!exchange.isResponseStarted()) {
                        writeBestEffort(exchange, new ServiceUnavailableException("http handler interrupted", e));
                    }
                } finally {
                    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                    recordEndpointLatency(endpoint, elapsedMillis);
                    endpoint.inflight().decrementAndGet();
                    releaseBudgets(routeBudget, endpointBudget);
                }
            });
        };
    }

    private boolean tryAcquireBudgets(HttpServerExchange exchange,
                                      String operation,
                                      RouteBudget routeBudget,
                                      EndpointBudget endpointBudget,
                                      EndpointStats endpoint,
                                      boolean requireExecutorCapacity) {
        if (requireExecutorCapacity
                && requestExecutor.getActiveCount() >= requestExecutor.getMaximumPoolSize()
                && requestExecutor.getQueue().remainingCapacity() == 0) {
            globalOverloadCount.incrementAndGet();
            routeBudget.overloadCount().incrementAndGet();
            endpoint.overloadCount().incrementAndGet();
            writeBestEffort(exchange, new OverloadedException(
                    "http request executor is saturated; workers=" + requestExecutor.getMaximumPoolSize()
                            + " queueCapacity=" + requestExecutorQueueCapacity
            ));
            return false;
        }
        if (!requestSlots.tryAcquire()) {
            globalOverloadCount.incrementAndGet();
            routeBudget.overloadCount().incrementAndGet();
            endpoint.overloadCount().incrementAndGet();
            writeBestEffort(exchange, new OverloadedException(
                    "too many in-flight requests; limit=" + maxConcurrentRequests
            ));
            return false;
        }
        if (!routeBudget.slots().tryAcquire()) {
            routeBudget.overloadCount().incrementAndGet();
            writeBestEffort(exchange, new OverloadedException(
                    operation + " route budget exhausted; limit=" + routeBudget.maxConcurrentRequests()
            ));
            requestSlots.release();
            return false;
        }
        if (endpointBudget != null && !endpointBudget.slots().tryAcquire()) {
            endpoint.overloadCount().incrementAndGet();
            writeBestEffort(exchange, new OverloadedException(
                    operation + " endpoint budget exhausted; limit=" + endpointBudget.maxConcurrentRequests()
            ));
            routeBudget.slots().release();
            requestSlots.release();
            return false;
        }
        return true;
    }

    private void releaseBudgets(RouteBudget routeBudget, EndpointBudget endpointBudget) {
        if (endpointBudget != null) {
            endpointBudget.slots().release();
        }
        routeBudget.slots().release();
        requestSlots.release();
    }

    private static final class LimitedBodyInputStream extends InputStream {
        private final InputStream delegate;
        private final int maxBodyBytes;
        private int totalRead;

        private LimitedBodyInputStream(InputStream delegate, int maxBodyBytes) {
            this.delegate = delegate;
            this.maxBodyBytes = maxBodyBytes;
        }

        @Override
        public int read() throws IOException {
            int value = delegate.read();
            if (value >= 0) {
                increment(1);
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int read = delegate.read(b, off, len);
            if (read > 0) {
                increment(read);
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private void increment(int read) throws IOException {
            totalRead += read;
            if (totalRead > maxBodyBytes) {
                throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
            }
        }
    }

    private void writeBestEffort(HttpServerExchange exchange, IOException error) {
        if (exchange.isResponseStarted()) {
            return;
        }
        try {
            handleServiceFailure(exchange, "http handler", error);
        } catch (IOException ignored) {
            exchange.setStatusCode(500);
            exchange.endExchange();
        }
    }

    private void writeBestEffort(HttpServerExchange exchange, RuntimeException error) {
        if (exchange.isResponseStarted()) {
            return;
        }
        try {
            handleApiFailure(exchange, "http handler", error);
        } catch (IOException ignored) {
            exchange.setStatusCode(500);
            exchange.endExchange();
        }
    }

    private ResponseCommitGuard responseGuard(HttpServerExchange exchange) {
        ResponseCommitGuard guard = exchange.getAttachment(RESPONSE_GUARD);
        if (guard == null) {
            guard = new ResponseCommitGuard();
            exchange.putAttachment(RESPONSE_GUARD, guard);
        }
        return guard;
    }

    private void appendRouteMetrics(StringBuilder builder, RouteBudget routeBudget) {
        int inflight = routeBudget.maxConcurrentRequests() - routeBudget.slots().availablePermits();
        double saturation = routeBudget.maxConcurrentRequests() == 0
                ? 0.0d
                : ((double) inflight) / routeBudget.maxConcurrentRequests();
        builder.append("ull_matcher_http_route_requests_total{route=\"").append(routeBudget.name()).append("\"} ")
                .append(routeBudget.requestCount().get()).append('\n')
                .append("ull_matcher_http_route_overload_total{route=\"").append(routeBudget.name()).append("\"} ")
                .append(routeBudget.overloadCount().get()).append('\n')
                .append("ull_matcher_http_route_timeout_total{route=\"").append(routeBudget.name()).append("\"} ")
                .append(routeBudget.timeoutCount().get()).append('\n')
                .append("ull_matcher_http_route_inflight{route=\"").append(routeBudget.name()).append("\"} ")
                .append(inflight).append('\n')
                .append("ull_matcher_http_route_saturation{route=\"").append(routeBudget.name()).append("\"} ")
                .append(saturation).append('\n');
    }

    private void appendEndpointMetrics(StringBuilder builder, String endpoint, EndpointStats stats) {
        double saturationShare = stats.routeMaxConcurrentRequests() == 0
                ? 0.0d
                : ((double) stats.inflight().get()) / stats.routeMaxConcurrentRequests();
        double endpointBudgetSaturation = stats.endpointMaxConcurrentRequests() == 0
                ? 0.0d
                : ((double) stats.inflight().get()) / stats.endpointMaxConcurrentRequests();
        builder.append("ull_matcher_http_endpoint_requests_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.requestCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_overload_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.overloadCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_timeouts_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.timeoutCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_failures_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.failureCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_inflight{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(stats.inflight().get()).append('\n')
                .append("ull_matcher_http_endpoint_max_inflight{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(stats.maxInflight().get()).append('\n')
                .append("ull_matcher_http_endpoint_budget_saturation{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(endpointBudgetSaturation).append('\n')
                .append("ull_matcher_http_endpoint_route_saturation_share{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(saturationShare).append('\n')
                .append("ull_matcher_http_endpoint_duration_ms_sum{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.durationSumMillis().get()).append('\n')
                .append("ull_matcher_http_endpoint_duration_ms_max{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.durationMaxMillis().get()).append('\n');
        long[] bucketBoundaries = ENDPOINT_LATENCY_BUCKETS_MILLIS;
        for (int i = 0; i < bucketBoundaries.length; i++) {
            builder.append("ull_matcher_http_endpoint_duration_bucket{endpoint=\"")
                    .append(endpoint)
                    .append("\",le=\"")
                    .append(bucketBoundaries[i])
                    .append("\"} ")
                    .append(stats.bucketCounts()[i].get())
                    .append('\n');
        }
        builder.append("ull_matcher_http_endpoint_duration_bucket{endpoint=\"")
                .append(endpoint)
                .append("\",le=\"+Inf\"} ")
                .append(stats.requestCount().get())
                .append('\n');
    }

    private String escapeLabelValue(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private void recordEndpointLatency(EndpointStats endpoint, long elapsedMillis) {
        endpoint.durationSumMillis().addAndGet(elapsedMillis);
        endpoint.durationMaxMillis().accumulateAndGet(elapsedMillis, Math::max);
        for (int i = 0; i < ENDPOINT_LATENCY_BUCKETS_MILLIS.length; i++) {
            if (elapsedMillis <= ENDPOINT_LATENCY_BUCKETS_MILLIS[i]) {
                endpoint.bucketCounts()[i].incrementAndGet();
            }
        }
    }

    @FunctionalInterface
    private interface BlockingExchangeHandler {
        void handle(HttpServerExchange exchange) throws IOException;
    }

    private static final class WrappedIOException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final IOException cause;

        private WrappedIOException(IOException cause) {
            super(cause);
            this.cause = cause;
        }
    }

    private static final class ResponseCommitGuard {
        private final AtomicInteger committed = new AtomicInteger();

        private boolean tryCommit() {
            return committed.compareAndSet(0, 1);
        }
    }

    private record RouteBudget(
            String name,
            int maxConcurrentRequests,
            Semaphore slots,
            long timeoutMillis,
            AtomicLong requestCount,
            AtomicLong overloadCount,
            AtomicLong timeoutCount
    ) {
        private RouteBudget(String name, int maxConcurrentRequests, Semaphore slots, long timeoutMillis) {
            this(name, maxConcurrentRequests, slots, timeoutMillis, new AtomicLong(), new AtomicLong(), new AtomicLong());
        }
    }

    private record EndpointStats(
            String endpointName,
            String routeName,
            int routeMaxConcurrentRequests,
            int endpointMaxConcurrentRequests,
            AtomicLong requestCount,
            AtomicLong overloadCount,
            AtomicLong timeoutCount,
            AtomicLong failureCount,
            AtomicLong inflight,
            AtomicLong maxInflight,
            AtomicLong durationSumMillis,
            AtomicLong durationMaxMillis,
            AtomicLong[] bucketCounts
    ) {
        private EndpointStats(String endpointName, String routeName, int routeMaxConcurrentRequests, int endpointMaxConcurrentRequests, int bucketCount) {
            this(endpointName, routeName, routeMaxConcurrentRequests, endpointMaxConcurrentRequests,
                    new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(),
                    new AtomicLong(), new AtomicLong(), newAtomicLongArray(bucketCount));
        }

        private static AtomicLong[] newAtomicLongArray(int size) {
            AtomicLong[] values = new AtomicLong[size];
            for (int i = 0; i < size; i++) {
                values[i] = new AtomicLong();
            }
            return values;
        }
    }

    private record EndpointBudget(String name, int maxConcurrentRequests, Semaphore slots) {}

    private record NewOrderRequest(long userId, long orderId, String side, String orderType, String timeInForce,
                                   long price, long quantity, Long ttlMillis, String idempotencyKey) {
    }

    private record CancelOrderRequest(long orderId, String idempotencyKey) {
    }
}
