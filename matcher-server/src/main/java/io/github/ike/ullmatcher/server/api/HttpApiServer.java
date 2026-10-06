package io.github.ike.ullmatcher.server.api;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.json.JsonMapper;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.github.ike.ullmatcher.server.engine.BatchReplicationAwait;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.OrderStateView;
import io.github.ike.ullmatcher.server.engine.SubmissionPhase;
import io.github.ike.ullmatcher.server.engine.SubmissionReceipt;
import io.github.ike.ullmatcher.server.engine.SubmissionView;
import io.github.ike.ullmatcher.server.telemetry.MatcherNodeMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.ReadinessSnapshot;
import io.github.ike.ullmatcher.orchestrator.SymbolRoute;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.util.Headers;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class HttpApiServer implements Closeable {
    private static final String METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";
    private static final String ROUTE_ORDERS = "/api/v1/orders";
    private static final String ROUTE_ORDERS_BATCH = "/api/v1/orders/batch";
    private static final String ROUTE_ORDER_BY_ID = "/api/v1/orders/{orderId}";
    private static final String ROUTE_CANCEL = "/api/v1/orders/cancel";
    private static final String ROUTE_SUBMISSION = "/api/v1/submissions/{submissionId}";
    private static final String ROUTE_SUBMISSION_BY_KEY = "/api/v1/submissions/by-idempotency";
    private static final String ROUTE_SNAPSHOT = "/api/v1/admin/snapshot";
    private static final String ROUTE_LIVE = "/api/v1/runtime/live";
    private static final String ROUTE_HEALTH = "/api/v1/runtime/health";
    private static final String ROUTE_STATE = "/api/v1/runtime/state";
    private static final String ROUTE_READINESS = "/api/v1/runtime/readiness";
    private static final String ROUTE_METRICS = "/metrics";
    private static final String ROUTE_ORCHESTRATOR_SYMBOL =
            "/api/v1/orchestrator/routes/symbols/{symbolId}";
    private static final String ROUTE_ORCHESTRATOR_SYMBOL_PREFIX =
            "/api/v1/orchestrator/routes/symbols/";
    private static final int DEFAULT_RECENT_ORDERS_LIMIT = 50;
    private static final int MAX_HTTP_ORDER_BATCH_SIZE = 1024;
    static final int METRICS_BUFFER_INITIAL_CAPACITY = 512;
    static final long[] ENDPOINT_LATENCY_BUCKETS_MILLIS = HttpRouteMetrics.ENDPOINT_LATENCY_BUCKETS_MILLIS;

    private final MatcherNodeService nodeService;
    private final GrpcTransportMetrics grpcMetrics;
    private final Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier;
    private final Supplier<ReadinessSnapshot> readinessSupplier;
    private final MatcherServerMode serverMode;
    private final IngressAuthConfig ingressAuthConfig;
    private final int maxBodyBytes;
    private final int maxConcurrentRequests;
    private final long requestTimeoutMillis;
    private final String bindHost;
    private final int requestedPort;
    private final RouteBudget writeBudget;
    private final RouteBudget readBudget;
    private final RouteBudget adminBudget;
    private final WriteAdmissionController writeAdmissionController;
    private final HttpEndpointBudget submitBudget;
    private final HttpEndpointBudget cancelBudget;
    private final HttpEndpointBudget snapshotBudget;
    private final HttpEndpointBudget readinessBudget;
    private final HttpEndpointBudget metricsBudget;
    private final HttpJsonCodec jsonCodec;
    private final HttpRequestPipeline requestPipeline;
    private final HttpBudgetGuard budgetGuard;
    private final Supplier<BinaryOrderIngressServer.BinaryIngressConnectionMetrics> binaryIngressMetrics;
    private final HttpSubmitAckMode defaultSubmitAckMode;
    private final int submitBatchMaxOrders;
    private final OrchestratorRouteLookup orchestratorRouteLookup;
    private final JsonMapper objectMapper = JsonMapper.builderWithJackson2Defaults().build();
    private final ObjectReader newOrderRequestReader = objectMapper.readerFor(NewOrderRequest.class);
    private final ObjectReader newOrderBatchRequestReader = objectMapper.readerFor(NewOrderBatchRequest.class);
    private final ObjectReader cancelOrderRequestReader = objectMapper.readerFor(CancelOrderRequest.class);
    private final HttpDispatchExecutor dispatchExecutor;
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
                HttpSubmitAckMode.LOCAL, MatcherServerMode.DEV, IngressAuthConfig.disabled(), nodeService, grpcMetrics, clusterMetricsSupplier, readinessSupplier);
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
                defaultSubmitAckMode, MatcherServerMode.DEV, IngressAuthConfig.disabled(), nodeService, grpcMetrics, clusterMetricsSupplier, readinessSupplier);
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
                         IngressAuthConfig ingressAuthConfig,
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
                defaultSubmitAckMode, serverMode, ingressAuthConfig, nodeService, grpcMetrics,
                clusterMetricsSupplier, readinessSupplier,
                BinaryOrderIngressServer.BinaryIngressConnectionMetrics::none);
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
                         IngressAuthConfig ingressAuthConfig,
                         MatcherNodeService nodeService,
                         GrpcTransportMetrics grpcMetrics,
                         Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier,
                         Supplier<ReadinessSnapshot> readinessSupplier,
                         Supplier<BinaryOrderIngressServer.BinaryIngressConnectionMetrics> binaryIngressMetrics) {
        this(port, bindHost, workerThreads, maxBodyBytes, maxConcurrentRequests, requestTimeoutMillis,
                writeMaxConcurrentRequests, readMaxConcurrentRequests, adminMaxConcurrentRequests,
                writeTimeoutMillis, readTimeoutMillis, adminTimeoutMillis,
                submitEndpointMaxConcurrentRequests, cancelEndpointMaxConcurrentRequests,
                snapshotEndpointMaxConcurrentRequests, readinessEndpointMaxConcurrentRequests,
                metricsEndpointMaxConcurrentRequests, shardKey, writeAdmissionPolicyConfig,
                defaultSubmitAckMode, serverMode, ingressAuthConfig, nodeService, grpcMetrics,
                clusterMetricsSupplier, readinessSupplier, binaryIngressMetrics, null);
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
                         IngressAuthConfig ingressAuthConfig,
                         MatcherNodeService nodeService,
                         GrpcTransportMetrics grpcMetrics,
                         Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier,
                         Supplier<ReadinessSnapshot> readinessSupplier,
                         Supplier<BinaryOrderIngressServer.BinaryIngressConnectionMetrics> binaryIngressMetrics,
                         OrchestratorRouteLookup orchestratorRouteLookup) {
        this.nodeService = Objects.requireNonNull(nodeService, "nodeService");
        this.grpcMetrics = Objects.requireNonNull(grpcMetrics, "grpcMetrics");
        this.clusterMetricsSupplier = Objects.requireNonNull(clusterMetricsSupplier, "clusterMetricsSupplier");
        this.readinessSupplier = Objects.requireNonNull(readinessSupplier, "readinessSupplier");
        this.defaultSubmitAckMode = Objects.requireNonNull(defaultSubmitAckMode, "defaultSubmitAckMode");
        this.serverMode = Objects.requireNonNull(serverMode, "serverMode");
        this.ingressAuthConfig = Objects.requireNonNull(ingressAuthConfig, "ingressAuthConfig");
        this.binaryIngressMetrics = Objects.requireNonNull(binaryIngressMetrics, "binaryIngressMetrics");
        this.orchestratorRouteLookup = orchestratorRouteLookup;
        this.jsonCodec = new HttpJsonCodec(objectMapper, this.serverMode);
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
        this.submitBudget = HttpEndpointBudget.create("submit_order", submitEndpointMaxConcurrentRequests);
        this.cancelBudget = HttpEndpointBudget.create("cancel_order", cancelEndpointMaxConcurrentRequests);
        this.snapshotBudget = HttpEndpointBudget.create("create_snapshot", snapshotEndpointMaxConcurrentRequests);
        this.readinessBudget = HttpEndpointBudget.create("runtime_readiness", readinessEndpointMaxConcurrentRequests);
        this.metricsBudget = HttpEndpointBudget.create("metrics", metricsEndpointMaxConcurrentRequests);
        this.dispatchExecutor = HttpDispatchExecutor.create(workerThreads, maxConcurrentRequests);
        this.requestSlots = new Semaphore(maxConcurrentRequests);
        this.budgetGuard = new HttpBudgetGuard(
                dispatchExecutor, requestSlots, maxConcurrentRequests, globalOverloadCount, jsonCodec);
        HttpAuthFilter authFilter = new HttpAuthFilter(this.ingressAuthConfig, jsonCodec);
        this.requestPipeline = new HttpRequestPipeline(budgetGuard, authFilter, jsonCodec, dispatchExecutor, endpointStats);
        RoutingHandler routes = Handlers.routing()
                .get(ROUTE_ORDERS, requestPipeline.blocking("recent_orders", "recent orders", readBudget, null, true, this::handleRecentOrders))
                .get(ROUTE_ORDER_BY_ID, requestPipeline.blocking("get_order", "get order", readBudget, null, true, this::handleGetOrder))
                .post(ROUTE_ORDERS, requestPipeline.directBlocking("submit_order", "submit order", writeBudget, submitBudget, true, this::handleSubmitOrder))
                .post(ROUTE_ORDERS_BATCH, requestPipeline.directBlocking("submit_order_batch", "submit order batch", writeBudget, submitBudget, true, this::handleSubmitOrderBatch))
                .post(ROUTE_CANCEL, requestPipeline.directBlocking("cancel_order", "cancel order", writeBudget, cancelBudget, true, this::handleCancelOrder))
                .get(ROUTE_SUBMISSION, requestPipeline.blocking("get_submission", "get submission", readBudget, null, true, this::handleGetSubmission))
                .get(ROUTE_SUBMISSION_BY_KEY, requestPipeline.blocking("get_submission_by_key", "get submission by idempotency key", readBudget, null, true, this::handleGetSubmissionByKey))
                .post(ROUTE_SNAPSHOT, requestPipeline.blocking("create_snapshot", "create snapshot", adminBudget, snapshotBudget, true, this::handleCreateSnapshot))
                .get(ROUTE_LIVE, requestPipeline.blocking("runtime_live", "runtime liveness", readBudget, null, false, this::handleLiveness))
                .get(ROUTE_HEALTH, requestPipeline.blocking("runtime_health", "runtime health", readBudget, null, true, this::handleHealth))
                .get(ROUTE_STATE, requestPipeline.blocking("runtime_state", "runtime state", readBudget, null, true, this::handleHealth))
                .get(ROUTE_READINESS, requestPipeline.blocking("runtime_readiness", "runtime readiness", readBudget, readinessBudget, true, this::handleReadiness));
        if (orchestratorRouteLookup != null) {
            routes = routes.get(ROUTE_ORCHESTRATOR_SYMBOL,
                    requestPipeline.blocking(
                            "orchestrator_symbol_route",
                            "orchestrator symbol route",
                            readBudget,
                            null,
                            true,
                            this::handleOrchestratorSymbolRoute));
        }
        routes = routes.get(ROUTE_METRICS, requestPipeline.blocking("metrics", "metrics", readBudget, metricsBudget, true, this::handleMetrics))
                .setFallbackHandler(this::handleNotFound);
        int ioThreads = Math.max(4, Math.min(16, Math.max(2, workerThreads / 4)));
        int undertowWorkers = Math.max(workerThreads, ioThreads);
        this.server = Undertow.builder()
                .setIoThreads(ioThreads)
                .setWorkerThreads(undertowWorkers)
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
        dispatchExecutor.close();
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
        ArrayList<HttpBatchSubmissionResponseWriter.BatchEntry> submissions = new ArrayList<>(request.orders().size());
        ArrayList<BatchSubmitWork> enqueued = new ArrayList<>(request.orders().size());
        int accepted = 0;
        int failed = 0;
        for (int index = 0; index < request.orders().size(); index++) {
            NewOrderRequest order = request.orders().get(index);
            try {
                enqueued.add(enqueueSubmitOrder(exchange, order, index));
            } catch (IOException e) {
                failed++;
                ServerApiException apiError = new ServiceUnavailableException("submit order batch item failed", e);
                HttpJsonCodec.logApiFailure(apiError, e);
                submissions.add(new HttpBatchSubmissionResponseWriter.ErrorEntry(
                        index, apiError.statusCode(), apiError.errorCode(), apiError.getMessage()));
            } catch (RuntimeException e) {
                failed++;
                ServerApiException apiError = HttpApiExceptionMapper.map("submit order batch item", e);
                HttpJsonCodec.logApiFailure(apiError, e);
                submissions.add(new HttpBatchSubmissionResponseWriter.ErrorEntry(
                        index, apiError.statusCode(), apiError.errorCode(), apiError.getMessage()));
            }
        }
        boolean pending = false;
        HttpSubmitAckMode batchAckMode = HttpSubmitRequestPolicy.resolveAckMode(exchange, request.ack(), defaultSubmitAckMode);
        if (batchAckMode == HttpSubmitAckMode.COMMITTED && !enqueued.isEmpty()) {
            pending = awaitBatchCommitted(enqueued, submissions);
            accepted = (int) submissions.stream().filter(HttpBatchSubmissionResponseWriter.SuccessEntry.class::isInstance).count();
            failed = (int) submissions.stream().filter(HttpBatchSubmissionResponseWriter.ErrorEntry.class::isInstance).count();
        } else {
            for (BatchSubmitWork work : enqueued) {
                try {
                    SubmissionReceipt receipt = awaitSubmission(
                            exchange,
                            work.handle(),
                            work.order().ack() == null ? request.ack() : work.order().ack());
                    int itemStatus = submissionStatusCode(receipt);
                    pending |= itemStatus == 202;
                    accepted++;
                    submissions.add(new HttpBatchSubmissionResponseWriter.SuccessEntry(work.index(), itemStatus, receipt));
                } catch (IOException e) {
                    failed++;
                    ServerApiException apiError = new ServiceUnavailableException("submit order batch item failed", e);
                    HttpJsonCodec.logApiFailure(apiError, e);
                    submissions.add(new HttpBatchSubmissionResponseWriter.ErrorEntry(
                            work.index(), apiError.statusCode(), apiError.errorCode(), apiError.getMessage()));
                } catch (RuntimeException e) {
                    failed++;
                    ServerApiException apiError = HttpApiExceptionMapper.map("submit order batch item", e);
                    HttpJsonCodec.logApiFailure(apiError, e);
                    submissions.add(new HttpBatchSubmissionResponseWriter.ErrorEntry(
                            work.index(), apiError.statusCode(), apiError.errorCode(), apiError.getMessage()));
                }
            }
        }
        submissions.sort(java.util.Comparator.comparingInt(entry -> switch (entry) {
            case HttpBatchSubmissionResponseWriter.SuccessEntry success -> success.index();
            case HttpBatchSubmissionResponseWriter.ErrorEntry error -> error.index();
        }));
        int status = failed > 0 ? 207 : (pending ? 202 : 200);
        HttpBatchSubmissionResponseWriter.write(
                exchange,
                jsonCodec,
                status,
                accepted,
                failed,
                submissions,
                ROUTE_SUBMISSION,
                ROUTE_SUBMISSION_BY_KEY);
    }

    private record BatchSubmitWork(int index, NewOrderRequest order,
                                   io.github.ike.ullmatcher.server.engine.SubmissionTracker.SubmissionHandle handle) {
    }

    private boolean awaitBatchCommitted(ArrayList<BatchSubmitWork> enqueued,
                                        ArrayList<HttpBatchSubmissionResponseWriter.BatchEntry> submissions) throws IOException {
        io.github.ike.ullmatcher.server.engine.SubmissionTracker.SubmissionHandle[] handles =
                new io.github.ike.ullmatcher.server.engine.SubmissionTracker.SubmissionHandle[enqueued.size()];
        for (int i = 0; i < enqueued.size(); i++) {
            handles[i] = enqueued.get(i).handle();
        }
        SubmissionReceipt[] receipts = new SubmissionReceipt[handles.length];
        BatchReplicationAwait.awaitCommittedReceipts(handles, writeBudget.timeoutMillis(), receipts);
        boolean pending = false;
        for (int i = 0; i < enqueued.size(); i++) {
            BatchSubmitWork work = enqueued.get(i);
            SubmissionReceipt receipt = receipts[i];
            int itemStatus = submissionStatusCode(receipt);
            pending |= itemStatus == 202;
            submissions.add(new HttpBatchSubmissionResponseWriter.SuccessEntry(work.index(), itemStatus, receipt));
        }
        return pending;
    }

    private SubmissionReceipt submitOrder(HttpServerExchange exchange, NewOrderRequest request, String batchAckMode) throws IOException {
        BatchSubmitWork work = enqueueSubmitOrder(exchange, request, 0);
        return awaitSubmission(exchange, work.handle(), request.ack() == null ? batchAckMode : request.ack());
    }

    private BatchSubmitWork enqueueSubmitOrder(HttpServerExchange exchange, NewOrderRequest request, int index) throws IOException {
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
                    HttpOrderEnums.parseSide(request.side()),
                    HttpOrderEnums.parseOrderType(request.orderType()),
                    HttpOrderEnums.parseTimeInForce(request.timeInForce()),
                    request.price(),
                    request.quantity(),
                    request.ttlMillis(),
                    idempotencyKey
            );
            return new BatchSubmitWork(index, request, handle);
        }
    }

    private void handleOrchestratorSymbolRoute(HttpServerExchange exchange) throws IOException {
        String symbolIdText = exchange.getPathParameters().containsKey("symbolId")
                ? exchange.getPathParameters().get("symbolId").getFirst()
                : orchestratorSymbolIdFromPath(exchange.getRequestPath());
        if (symbolIdText == null || symbolIdText.isBlank()) {
            writeJson(exchange, 400, Map.of("error", "missing symbolId"));
            return;
        }
        try {
            int symbolId = Integer.parseInt(symbolIdText);
            if (symbolId <= 0) {
                writeJson(exchange, 400, Map.of("error", "invalid symbolId", "symbolId", symbolIdText));
                return;
            }
            Optional<SymbolRoute> route = orchestratorRouteLookup.lookup(symbolId);
            if (route.isEmpty()) {
                writeJson(exchange, 404, Map.of("error", "symbol route not found", "symbolId", symbolId));
                return;
            }
            writeJson(exchange, 200, HttpOrchestratorRouteDocument.toResponseBody(route.get()));
        } catch (NumberFormatException e) {
            handleApiFailure(exchange, "orchestrator symbol route", new BadRequestException("invalid symbolId", e));
        } catch (IOException e) {
            writeJson(exchange, 503, Map.of("error", "orchestrator route lookup failed"));
        } catch (RuntimeException e) {
            handleApiFailure(exchange, "orchestrator symbol route", e);
        }
    }

    private static String orchestratorSymbolIdFromPath(String requestPath) {
        if (requestPath == null || !requestPath.startsWith(ROUTE_ORCHESTRATOR_SYMBOL_PREFIX)) {
            return null;
        }
        String suffix = requestPath.substring(ROUTE_ORCHESTRATOR_SYMBOL_PREFIX.length());
        int slash = suffix.indexOf('/');
        return slash < 0 ? suffix : suffix.substring(0, slash);
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

    /**
     * Unauthenticated liveness probe. Deliberately returns no cluster, role or sequence detail so
     * that binding the listener to a non-loopback address does not leak internal state.
     */
    private void handleLiveness(HttpServerExchange exchange) throws IOException {
        writeJson(exchange, 200, Map.of("status", "UP"));
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
                            budgetGuard.maxConcurrentRequests(),
                            budgetGuard.availablePermits(),
                            budgetGuard.executorQueueDepth(),
                            budgetGuard.executorQueueCapacity(),
                            budgetGuard.globalOverloadCount(),
                            writeAdmissionController,
                            List.of(writeBudget, readBudget, adminBudget),
                            endpointStats
                    ),
                    binaryIngressMetrics.get()
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
        jsonCodec.handleServiceFailure(exchange, operation, error);
    }

    static Map<String, Object> serviceFailurePayload(MatcherServerMode serverMode, ServerApiException apiError, Throwable error) {
        return HttpJsonCodec.serviceFailurePayload(serverMode, apiError, error);
    }

    private void handleApiFailure(HttpServerExchange exchange, String operation, RuntimeException error) throws IOException {
        jsonCodec.handleApiFailure(exchange, operation, error);
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
        exchange.getResponseHeaders().put(Headers.LOCATION, "/api/v1/submissions/" + receipt.submissionId());
        HttpSubmissionReceiptWriter.writeReceipt(
                exchange,
                jsonCodec,
                submissionStatusCode(receipt),
                receipt,
                ROUTE_SUBMISSION,
                ROUTE_SUBMISSION_BY_KEY);
    }

    private SubmissionReceipt awaitSubmission(HttpServerExchange exchange,
                                              io.github.ike.ullmatcher.server.engine.SubmissionTracker.SubmissionHandle handle,
                                              String bodyAckMode) throws IOException {
        HttpSubmitAckMode ackMode = HttpSubmitRequestPolicy.resolveAckMode(exchange, bodyAckMode, defaultSubmitAckMode);
        if (ackMode == HttpSubmitAckMode.LOCAL) {
            return handle.awaitLocalReceipt(writeBudget.timeoutMillis());
        }
        return handle.awaitCommittedReceipt(writeBudget.timeoutMillis());
    }

    private Map<String, Object> submissionPayload(SubmissionView view) {
        return SubmissionPayloads.fromView(view, ROUTE_SUBMISSION, ROUTE_SUBMISSION_BY_KEY);
    }

    private NewOrderRequest readNewOrderRequest(HttpServerExchange exchange) throws IOException {
        byte[] body = readBodyBytes(exchange);
        NewOrderRequest fast = HttpNewOrderFastParser.tryParse(body, body.length);
        if (fast != null) {
            return fast;
        }
        try {
            return newOrderRequestReader.readValue(body);
        } catch (JacksonException e) {
            throw new BadRequestException("invalid request body", e);
        }
    }

    private NewOrderBatchRequest readNewOrderBatchRequest(HttpServerExchange exchange) throws IOException {
        byte[] body = readBodyBytes(exchange);
        NewOrderBatchRequest fast = HttpNewOrderBatchFastParser.tryParse(body, body.length);
        if (fast != null) {
            return fast;
        }
        try {
            return newOrderBatchRequestReader.readValue(body);
        } catch (JacksonException e) {
            throw new BadRequestException("invalid request body", e);
        }
    }

    private CancelOrderRequest readCancelOrderRequest(HttpServerExchange exchange) throws IOException {
        return readRequest(exchange, cancelOrderRequestReader);
    }

    private byte[] readBodyBytes(HttpServerExchange exchange) throws IOException {
        exchange.startBlocking();
        long declaredLength = exchange.getRequestContentLength();
        if (declaredLength > maxBodyBytes) {
            throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
        }
        try (InputStream body = new RequestBodyLimitInputStream(exchange.getInputStream(), maxBodyBytes)) {
            return HttpLimitedBodies.readAll(body, maxBodyBytes, declaredLength);
        }
    }

    private <T> T readRequest(HttpServerExchange exchange, ObjectReader reader) throws IOException {
        try {
            return reader.readValue(readBodyBytes(exchange));
        } catch (JacksonException e) {
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
        jsonCodec.writeJson(exchange, status, body);
    }
}
