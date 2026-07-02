package io.github.ike.ullmatcher.server.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
    private static final String ROUTE_ORDERS_BATCH = "/api/v1/orders/batch";
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
    private static final int MAX_HTTP_ORDER_BATCH_SIZE = 1024;
    static final int METRICS_BUFFER_INITIAL_CAPACITY = 512;
    private static final long CLOSE_TIMEOUT_SECONDS = 5L;
    static final long[] ENDPOINT_LATENCY_BUCKETS_MILLIS = HttpRouteMetrics.ENDPOINT_LATENCY_BUCKETS_MILLIS;
    private static final AttachmentKey<ResponseCommitGuard> RESPONSE_GUARD = AttachmentKey.create(ResponseCommitGuard.class);

    private final MatcherNodeService nodeService;
    private final GrpcTransportMetrics grpcMetrics;
    private final Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier;
    private final Supplier<ReadinessSnapshot> readinessSupplier;
    private final MatcherServerMode serverMode;
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
    private final HttpSubmitAckMode defaultSubmitAckMode;
    private final int submitBatchMaxOrders;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectReader newOrderRequestReader = objectMapper.readerFor(NewOrderRequest.class);
    private final ObjectReader newOrderBatchRequestReader = objectMapper.readerFor(NewOrderBatchRequest.class);
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
        this(port, bindHost, workerThreads, maxBodyBytes, maxConcurrentRequests, requestTimeoutMillis,
                writeMaxConcurrentRequests, readMaxConcurrentRequests, adminMaxConcurrentRequests,
                writeTimeoutMillis, readTimeoutMillis, adminTimeoutMillis,
                submitEndpointMaxConcurrentRequests, cancelEndpointMaxConcurrentRequests,
                snapshotEndpointMaxConcurrentRequests, readinessEndpointMaxConcurrentRequests,
                metricsEndpointMaxConcurrentRequests, shardKey, writeAdmissionPolicyConfig,
                HttpSubmitAckMode.LOCAL, MatcherServerMode.DEV, nodeService, grpcMetrics, clusterMetricsSupplier, readinessSupplier);
    }

    public HttpApiServer(int port, String bindHost, int workerThreads, int maxBodyBytes, int maxConcurrentRequests, long requestTimeoutMillis,
                         int writeMaxConcurrentRequests, int readMaxConcurrentRequests, int adminMaxConcurrentRequests,
                         long writeTimeoutMillis, long readTimeoutMillis, long adminTimeoutMillis,
                         int submitEndpointMaxConcurrentRequests, int cancelEndpointMaxConcurrentRequests,
                         int snapshotEndpointMaxConcurrentRequests, int readinessEndpointMaxConcurrentRequests,
                         int metricsEndpointMaxConcurrentRequests,
                         String shardKey, WriteAdmissionPolicyConfig writeAdmissionPolicyConfig,
                         HttpSubmitAckMode defaultSubmitAckMode,
                         MatcherNodeService nodeService,
                         GrpcTransportMetrics grpcMetrics,
                         Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier,
                         Supplier<ReadinessSnapshot> readinessSupplier) {
        this(port, bindHost, workerThreads, maxBodyBytes, maxConcurrentRequests, requestTimeoutMillis,
                writeMaxConcurrentRequests, readMaxConcurrentRequests, adminMaxConcurrentRequests,
                writeTimeoutMillis, readTimeoutMillis, adminTimeoutMillis,
                submitEndpointMaxConcurrentRequests, cancelEndpointMaxConcurrentRequests,
                snapshotEndpointMaxConcurrentRequests, readinessEndpointMaxConcurrentRequests,
                metricsEndpointMaxConcurrentRequests, shardKey, writeAdmissionPolicyConfig,
                defaultSubmitAckMode, MatcherServerMode.DEV, nodeService, grpcMetrics, clusterMetricsSupplier, readinessSupplier);
    }

    public HttpApiServer(int port, String bindHost, int workerThreads, int maxBodyBytes, int maxConcurrentRequests, long requestTimeoutMillis,
                         int writeMaxConcurrentRequests, int readMaxConcurrentRequests, int adminMaxConcurrentRequests,
                         long writeTimeoutMillis, long readTimeoutMillis, long adminTimeoutMillis,
                         int submitEndpointMaxConcurrentRequests, int cancelEndpointMaxConcurrentRequests,
                         int snapshotEndpointMaxConcurrentRequests, int readinessEndpointMaxConcurrentRequests,
                         int metricsEndpointMaxConcurrentRequests,
                         String shardKey, WriteAdmissionPolicyConfig writeAdmissionPolicyConfig,
                         HttpSubmitAckMode defaultSubmitAckMode,
                         MatcherServerMode serverMode,
                         MatcherNodeService nodeService,
                         GrpcTransportMetrics grpcMetrics,
                         Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier,
                         Supplier<ReadinessSnapshot> readinessSupplier) {
        this.nodeService = Objects.requireNonNull(nodeService, "nodeService");
        this.grpcMetrics = Objects.requireNonNull(grpcMetrics, "grpcMetrics");
        this.clusterMetricsSupplier = Objects.requireNonNull(clusterMetricsSupplier, "clusterMetricsSupplier");
        this.readinessSupplier = Objects.requireNonNull(readinessSupplier, "readinessSupplier");
        this.defaultSubmitAckMode = Objects.requireNonNull(defaultSubmitAckMode, "defaultSubmitAckMode");
        this.serverMode = Objects.requireNonNull(serverMode, "serverMode");
        this.submitBatchMaxOrders = Math.max(1, Integer.getInteger("matcher.httpSubmitBatchMaxOrders", MAX_HTTP_ORDER_BATCH_SIZE));
        this.maxBodyBytes = maxBodyBytes;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost");
        this.requestedPort = port;
        int requestThreads = Math.max(2, workerThreads);
        this.writeBudget = RouteBudget.create("write", writeMaxConcurrentRequests, writeTimeoutMillis);
        this.readBudget = RouteBudget.create("read", readMaxConcurrentRequests, readTimeoutMillis);
        this.adminBudget = RouteBudget.create("admin", adminMaxConcurrentRequests, adminTimeoutMillis);
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
                .post(ROUTE_ORDERS_BATCH, directBlocking("submit_order_batch", "submit order batch", writeBudget, submitBudget, this::handleSubmitOrderBatch))
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
            writeSubmissionResponse(exchange, submitOrder(exchange, request, null));
        } catch (IOException e) {
            handleServiceFailure(exchange, "submit order", e);
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "submit order", e);
        }
    }

    private void handleSubmitOrderBatch(HttpServerExchange exchange) throws IOException {
        NewOrderBatchRequest request = readNewOrderBatchRequest(exchange);
        if (request.orders() == null || request.orders().isEmpty()) {
            handleApiFailure(exchange, "submit order batch", new BadRequestException("orders must not be empty"));
            return;
        }
        if (request.orders().size() > submitBatchMaxOrders) {
            handleApiFailure(exchange, "submit order batch",
                    new BadRequestException("orders size exceeds max batch size " + submitBatchMaxOrders));
            return;
        }
        ArrayList<Map<String, Object>> submissions = new ArrayList<>(request.orders().size());
        int accepted = 0;
        int failed = 0;
        boolean pending = false;
        for (int index = 0; index < request.orders().size(); index++) {
            NewOrderRequest order = request.orders().get(index);
            try {
                SubmissionReceipt receipt = submitOrder(exchange, order, request.ack());
                int status = submissionStatusCode(receipt);
                pending |= status == 202;
                accepted++;
                Map<String, Object> payload = new LinkedHashMap<>(submissionReceiptPayload(receipt));
                payload.put("index", index);
                payload.put("status", status);
                submissions.add(payload);
            } catch (IOException e) {
                failed++;
                ServerApiException apiError = new ServiceUnavailableException("submit order batch item failed", e);
                logApiFailure(apiError, e);
                submissions.add(batchErrorPayload(index, apiError.statusCode(), apiError.errorCode(), apiError.getMessage()));
            } catch (RuntimeException e) {
                failed++;
                ServerApiException apiError = HttpApiExceptionMapper.map("submit order batch item", e);
                logApiFailure(apiError, e);
                submissions.add(batchErrorPayload(index, apiError.statusCode(), apiError.errorCode(), apiError.getMessage()));
            }
        }
        int status = failed > 0 ? 207 : (pending ? 202 : 200);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("accepted", accepted);
        payload.put("failed", failed);
        payload.put("count", submissions.size());
        payload.put("submissions", submissions);
        writeJson(exchange, status, payload);
    }

    private SubmissionReceipt submitOrder(HttpServerExchange exchange, NewOrderRequest request, String batchAckMode) throws IOException {
        WriteAdmissionController.Admission admission = writeAdmissionController.acquireForSubmit(exchange, request.userId());
        try (admission) {
            String idempotencyKey = HttpSubmitRequestPolicy.resolveIdempotencyKey(
                    exchange,
                    request.idempotencyKey(),
                    HttpSubmitRequestPolicy.defaultOrderIdempotencyKey(request.userId(), request.orderId())
            );
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
            return awaitSubmission(exchange, handle, request.ack() == null ? batchAckMode : request.ack());
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
                String idempotencyKey = HttpSubmitRequestPolicy.resolveIdempotencyKey(
                        exchange,
                        request.idempotencyKey(),
                        HttpSubmitRequestPolicy.defaultCancelIdempotencyKey(request.orderId())
                );
                var handle = nodeService.submitTrackedCancelOrder(request.orderId(), idempotencyKey);
                writeSubmissionResponse(exchange, awaitSubmission(exchange, handle, request.ack()));
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
            payload.put("submissionCommittedTotal", metrics.submissionMetrics().committedTotal());
            payload.put("submissionFailedTotal", metrics.submissionMetrics().failedTotal());
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
            String metrics = PrometheusMetricsExporter.export(
                    state,
                    nodeMetrics,
                    grpc,
                    cluster,
                    readiness,
                    new PrometheusMetricsExporter.HttpMetrics(
                            maxConcurrentRequests,
                            requestSlots.availablePermits(),
                            requestExecutor.getQueue().size(),
                            requestExecutorQueueCapacity,
                            globalOverloadCount.get(),
                            writeAdmissionController,
                            List.of(writeBudget, readBudget, adminBudget),
                            endpointStats
                    )
            );
            byte[] bytes = metrics.getBytes(StandardCharsets.UTF_8);
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
        writeJson(exchange, apiError.statusCode(), serviceFailurePayload(serverMode, apiError, error));
    }

    static Map<String, Object> serviceFailurePayload(MatcherServerMode serverMode, ServerApiException apiError, Throwable error) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("error", apiError.getMessage());
        payload.put("code", apiError.errorCode());
        if (serverMode != MatcherServerMode.PROD) {
            payload.put("detail", error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage());
        }
        return payload;
    }

    private void handleApiFailure(HttpServerExchange exchange, String operation, RuntimeException error) throws IOException {
        ServerApiException apiError = HttpApiExceptionMapper.map(operation, error);
        logApiFailure(apiError, error);
        writeJson(exchange, apiError.statusCode(), Map.of("error", apiError.getMessage(), "code", apiError.errorCode()));
    }

    private Map<String, Object> batchErrorPayload(int index, int status, String code, String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("index", index);
        payload.put("status", status);
        payload.put("code", code);
        payload.put("error", message);
        return payload;
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

    private SubmissionReceipt awaitSubmission(HttpServerExchange exchange,
                                              io.github.ike.ullmatcher.server.engine.SubmissionTracker.SubmissionHandle handle,
                                              String bodyAckMode) throws IOException {
        HttpSubmitAckMode ackMode = HttpSubmitRequestPolicy.resolveAckMode(exchange, bodyAckMode, defaultSubmitAckMode);
        if (ackMode == HttpSubmitAckMode.LOCAL) {
            return handle.awaitLocalReceipt(writeBudget.timeoutMillis());
        }
        SubmissionView committed = handle.awaitCommitted(writeBudget.timeoutMillis());
        return receiptFromView(committed);
    }

    private static SubmissionReceipt receiptFromView(SubmissionView view) {
        return new SubmissionReceipt(
                view.submissionId(),
                view.idempotencyKey(),
                view.operationType(),
                view.userId(),
                view.orderId(),
                view.sequence(),
                view.phase(),
                view.localResult(),
                view.localDurable(),
                view.replicationRequired(),
                view.replicationCommitted(),
                view.totalTargets(),
                view.requiredAcks(),
                view.ackedTargets(),
                view.retryCount(),
                view.lastError(),
                view.createdAtEpochMillis(),
                view.updatedAtEpochMillis()
        );
    }

    private Map<String, Object> submissionPayload(SubmissionView view) {
        return SubmissionPayloads.fromView(view, ROUTE_SUBMISSION, ROUTE_SUBMISSION_BY_KEY);
    }

    private Map<String, Object> submissionReceiptPayload(SubmissionReceipt receipt) {
        return SubmissionPayloads.fromReceipt(receipt, ROUTE_SUBMISSION, ROUTE_SUBMISSION_BY_KEY);
    }

    private NewOrderRequest readNewOrderRequest(HttpServerExchange exchange) throws IOException {
        return readRequest(exchange, newOrderRequestReader);
    }

    private NewOrderBatchRequest readNewOrderBatchRequest(HttpServerExchange exchange) throws IOException {
        return readRequest(exchange, newOrderBatchRequestReader);
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
        try (InputStream body = new RequestBodyLimitInputStream(exchange.getInputStream(), maxBodyBytes)) {
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
                    ignored -> EndpointStats.create(
                            endpointMetricKey,
                            routeBudget.name(),
                            routeBudget.maxConcurrentRequests(),
                            endpointBudget == null ? 0 : endpointBudget.maxConcurrentRequests())
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
                    ignored -> EndpointStats.create(
                            endpointMetricKey,
                            routeBudget.name(),
                            routeBudget.maxConcurrentRequests(),
                            endpointBudget == null ? 0 : endpointBudget.maxConcurrentRequests())
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

    private record EndpointBudget(String name, int maxConcurrentRequests, Semaphore slots) {}

}
