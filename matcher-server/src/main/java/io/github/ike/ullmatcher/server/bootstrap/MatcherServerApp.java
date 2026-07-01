package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationService;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.grpc.telemetry.OpenTelemetryGrpcMetricsBridge;
import io.github.ike.ullmatcher.server.api.BinaryOrderIngressServer;
import io.github.ike.ullmatcher.server.api.HttpApiServer;
import io.github.ike.ullmatcher.server.api.HttpSubmitAckMode;
import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterSupervisor;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyEnforcer;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportProvider;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportProviders;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.security.ReloadableGrpcServer;
import io.github.ike.ullmatcher.ha.transport.TransportSecuritySnapshot;
import io.github.ike.ullmatcher.server.telemetry.OpenTelemetryServerMetricsBridge;
import io.github.ike.ullmatcher.server.telemetry.ReadinessSnapshot;


import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class MatcherServerApp implements Closeable {
    private final MatcherNodeService nodeService;
    private final HttpApiServer httpApiServer;
    private final BinaryOrderIngressServer binaryIngressServer;
    private final ReloadableGrpcServer grpcServer;
    private final boolean grpcReplicationServerEnabled;
    private final MatcherClusterSupervisor clusterSupervisor;
    private final ReplicationTransportProvider replicationTransportProvider;
    private final OpenTelemetryGrpcMetricsBridge openTelemetryMetricsBridge;
    private final OpenTelemetryServerMetricsBridge openTelemetryServerMetricsBridge;
    private final GrpcTransportMetrics grpcMetrics;

    public MatcherServerApp(MatcherServerConfig config) throws IOException {
        config.validateDeploymentSafety();
        ReplicationTransportPolicyEnforcer.validateAndLock(config);
        this.grpcReplicationServerEnabled = config.requiresGrpcReplicationServer();
        this.nodeService = new MatcherNodeService(config);
        nodeService.start();
        this.grpcMetrics = new GrpcTransportMetrics();
        AtomicReference<MatcherClusterSupervisor> clusterSupervisorRef = new AtomicReference<>();
        Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier = () ->
                clusterSupervisorRef.get() == null
                        ? new ClusterSupervisorMetricsSnapshot(
                        0L, 0L, null, null, java.util.Map.of(), java.util.List.of(), "IDLE", "",
                        TransportMetricsSnapshot.none("NONE"))
                        : clusterSupervisorRef.get().metricsSnapshot();
        this.httpApiServer = new HttpApiServer(
                config.httpPort(),
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
                HttpSubmitAckMode.parse(System.getProperty("matcher.httpSubmitAckMode"), HttpSubmitAckMode.LOCAL),
                config.serverMode(),
                nodeService,
                grpcMetrics,
                clusterMetricsSupplier, this::readinessSnapshot);
        this.binaryIngressServer = config.binaryIngressEnabled()
                ? new BinaryOrderIngressServer(
                config.binaryIngressBindHost(),
                config.binaryIngressPort(),
                config.binaryIngressMaxBatchSize(),
                nodeService)
                : null;
        this.grpcServer = grpcReplicationServerEnabled
                ? new ReloadableGrpcServer(
                config::grpcServerConfig,
                () -> new GrpcReplicationService(
                        nodeService::standbySyncService,
                        nodeService,
                        nodeService,
                        grpcMetrics,
                        java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.grpcServerConfig().replicationIngressTimeoutMillis())
                ),
                config.securityConfig()
        )
                : null;
        this.openTelemetryMetricsBridge = config.securityConfig().openTelemetryMetricsEnabled()
                ? new OpenTelemetryGrpcMetricsBridge("ull-matcher-server", grpcMetrics)
                : null;
        this.replicationTransportProvider = config.clusterConfig() == null
                ? null
                : ReplicationTransportProviders.create(
                config.clusterConfig(),
                config.securityConfig(),
                nodeService);
        this.clusterSupervisor = config.clusterConfig() == null
                ? null
                : new MatcherClusterSupervisor(config, nodeService, replicationTransportProvider);
        clusterSupervisorRef.set(this.clusterSupervisor);
        this.openTelemetryServerMetricsBridge = config.securityConfig().openTelemetryMetricsEnabled()
                ? new OpenTelemetryServerMetricsBridge("ull-matcher-server", nodeService::metricsSnapshot, clusterMetricsSupplier)
                : null;
    }

    public void start() throws IOException {
        if (grpcServer != null) {
            grpcServer.start();
        }
        httpApiServer.start();
        if (binaryIngressServer != null) {
            binaryIngressServer.start();
        }
    }

    public int httpPort() {
        return httpApiServer.port();
    }

    public int grpcPort() {
        return grpcServer == null ? -1 : grpcServer.port();
    }

    public int binaryIngressPort() {
        return binaryIngressServer == null ? -1 : binaryIngressServer.port();
    }

    public MatcherNodeService nodeService() {
        return nodeService;
    }

    ReadinessSnapshot readinessSnapshot() {
        ClusterSupervisorMetricsSnapshot cluster = clusterSupervisor == null
                ? new ClusterSupervisorMetricsSnapshot(
                0L, 0L, null, null, java.util.Map.of(), java.util.List.of(), "IDLE", "",
                TransportMetricsSnapshot.none("NONE"))
                : clusterSupervisor.metricsSnapshot();
        TransportSecuritySnapshot tls = grpcServer != null
                ? grpcServer.snapshot()
                : (replicationTransportProvider == null
                ? new TransportSecuritySnapshot(0L, 0L, 0L, false, "")
                : replicationTransportProvider.securitySnapshot());
        boolean clientTrafficReady = nodeService.health().acceptingClientCommands();
        boolean promotionReady = cluster.lastGateDecision() == null || cluster.lastGateDecision().promotionReady();
        boolean snapshotSyncRequired = cluster.lastGateDecision() != null && cluster.lastGateDecision().snapshotSyncRequired();
        boolean catchUpInProgress = "CATCHING_UP".equals(cluster.syncState()) || "SNAPSHOT_SYNC".equals(cluster.syncState());
        boolean transportPolicyHealthy = !"DRIFT".equals(cluster.transportMetrics().policyStatus());
        boolean serviceReady = nodeService.health().loopState() != io.github.ike.ullmatcher.runtime.MatchLoopState.FAILED
                && !tls.reloading()
                && transportPolicyHealthy;
        ReadinessSnapshot.LastTickSummary lastTickResult = cluster.lastTickResult() == null
                ? null
                : new ReadinessSnapshot.LastTickSummary(
                cluster.lastTickResult().action().name(),
                cluster.lastTickResult().roleBefore().name(),
                cluster.lastTickResult().roleAfter().name(),
                cluster.lastTickResult().leaseChanged(),
                cluster.lastTickResult().reason()
        );
        ReadinessSnapshot.LastGateSummary lastGateDecision = cluster.lastGateDecision() == null
                ? null
                : new ReadinessSnapshot.LastGateSummary(
                cluster.lastGateDecision().promotionReady(),
                cluster.lastGateDecision().catchUpRequired(),
                cluster.lastGateDecision().snapshotSyncRequired(),
                cluster.lastGateDecision().report().reason(),
                cluster.lastGateDecision().report().receivedLag(),
                cluster.lastGateDecision().report().durableLag(),
                cluster.lastGateDecision().report().appliedLag(),
                cluster.lastGateDecision().report().snapshotLag()
        );
        String lastTickAction = cluster.lastTickResult() == null ? "NONE" : cluster.lastTickResult().action().name();
        String lastTickReason = cluster.lastTickResult() == null ? "" : cluster.lastTickResult().reason();
        String lastGateReason = cluster.lastGateDecision() == null ? "" : cluster.lastGateDecision().report().reason();
        long lastGateReceivedLag = cluster.lastGateDecision() == null ? 0L : cluster.lastGateDecision().report().receivedLag();
        long lastGateDurableLag = cluster.lastGateDecision() == null ? 0L : cluster.lastGateDecision().report().durableLag();
        long lastGateAppliedLag = cluster.lastGateDecision() == null ? 0L : cluster.lastGateDecision().report().appliedLag();
        long lastGateSnapshotLag = cluster.lastGateDecision() == null ? 0L : cluster.lastGateDecision().report().snapshotLag();
        String reason = !cluster.lastSyncError().isBlank()
                ? cluster.lastSyncError()
                : (!transportPolicyHealthy
                ? cluster.transportMetrics().policyConclusion()
                : (tls.lastError().isBlank() ? "ready" : tls.lastError()));
        return new ReadinessSnapshot(
                serviceReady,
                clientTrafficReady,
                promotionReady,
                snapshotSyncRequired,
                catchUpInProgress,
                tls.reloading(),
                tls.generation(),
                tls.reloadCount(),
                tls.failureCount(),
                tls.lastError(),
                cluster.syncState(),
                cluster.recentErrors(),
                lastTickResult,
                lastGateDecision,
                lastTickAction,
                lastTickReason,
                lastGateReason,
                lastGateReceivedLag,
                lastGateDurableLag,
                lastGateAppliedLag,
                lastGateSnapshotLag,
                cluster.transportMetrics().policyStatus(),
                cluster.transportMetrics().policyConclusion(),
                cluster.transportMetrics().reconciliationStatus(),
                cluster.transportMetrics().reconciliationConclusion(),
                reason
        );
    }

    @Override
    public void close() throws IOException {
        IOException error = null;
        try {
            httpApiServer.close();
        } catch (RuntimeException e) {
            error = new IOException("failed to stop HTTP API server", e);
        }
        try {
            if (binaryIngressServer != null) {
                binaryIngressServer.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        try {
            if (grpcServer != null) {
                grpcServer.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        try {
            if (clusterSupervisor != null) {
                clusterSupervisor.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        try {
            if (replicationTransportProvider != null) {
                replicationTransportProvider.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        try {
            if (openTelemetryMetricsBridge != null) {
                openTelemetryMetricsBridge.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        try {
            if (openTelemetryServerMetricsBridge != null) {
                openTelemetryServerMetricsBridge.close();
            }
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        try {
            nodeService.close();
        } catch (IOException e) {
            if (error == null) {
                error = e;
            } else {
                error.addSuppressed(e);
            }
        }
        if (error != null) {
            throw error;
        }
    }
}
