package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.runtime.MatchLoopConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportType;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;


import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public record MatcherServerConfig(
        MatcherServerMode serverMode,
        String nodeId,
        String shardKey,
        MatcherConfig matcherConfig,
        Path walDirectory,
        String walPrefix,
        long walSegmentSizeBytes,
        WalDurabilityMode walDurabilityMode,
        int walForceBatchSize,
        long walForceMaxDelayMicros,
        Path snapshotFile,
        int ringCapacity,
        int gatewaySpinLimit,
        long gatewayOfferTimeoutNanos,
        int httpPort,
        String httpBindHost,
        int httpWorkerThreads,
        int httpMaxBodyBytes,
        int httpMaxConcurrentRequests,
        long httpRequestTimeoutMillis,
        boolean binaryIngressEnabled,
        int binaryIngressPort,
        String binaryIngressBindHost,
        int binaryIngressMaxBatchSize,
        int httpWriteMaxConcurrentRequests,
        int httpReadMaxConcurrentRequests,
        int httpAdminMaxConcurrentRequests,
        long httpWriteTimeoutMillis,
        long httpReadTimeoutMillis,
        long httpAdminTimeoutMillis,
        int httpSubmitEndpointMaxConcurrentRequests,
        int httpCancelEndpointMaxConcurrentRequests,
        int httpSnapshotEndpointMaxConcurrentRequests,
        int httpReadinessEndpointMaxConcurrentRequests,
        int httpMetricsEndpointMaxConcurrentRequests,
        WriteAdmissionPolicyConfig writeAdmissionPolicyConfig,
        boolean allowInsecureRemoteHttp,
        int grpcPort,
        GrpcReplicationServerConfig grpcServerConfig,
        ServerSecurityConfig securityConfig,
        TtlCancelConfig ttlCancelConfig,
        HaRole initialRole,
        MatchLoopConfig loopConfig,
        StandbySyncConfig standbySyncConfig,
        MatcherClusterConfig clusterConfig
) {
    public MatcherServerConfig {
        Objects.requireNonNull(serverMode, "serverMode");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(shardKey, "shardKey");
        Objects.requireNonNull(matcherConfig, "matcherConfig");
        Objects.requireNonNull(walDirectory, "walDirectory");
        Objects.requireNonNull(walPrefix, "walPrefix");
        Objects.requireNonNull(snapshotFile, "snapshotFile");
        Objects.requireNonNull(walDurabilityMode, "walDurabilityMode");
        Objects.requireNonNull(initialRole, "initialRole");
        Objects.requireNonNull(grpcServerConfig, "grpcServerConfig");
        Objects.requireNonNull(securityConfig, "securityConfig");
        Objects.requireNonNull(ttlCancelConfig, "ttlCancelConfig");
        Objects.requireNonNull(loopConfig, "loopConfig");
        Objects.requireNonNull(standbySyncConfig, "standbySyncConfig");
        Objects.requireNonNull(writeAdmissionPolicyConfig, "writeAdmissionPolicyConfig");
        if (nodeId.isBlank() || shardKey.isBlank() || walPrefix.isBlank()
                || httpBindHost == null || httpBindHost.isBlank()
                || binaryIngressBindHost == null || binaryIngressBindHost.isBlank()) {
            throw new IllegalArgumentException("nodeId, shardKey, walPrefix, httpBindHost and binaryIngressBindHost must not be blank");
        }
        if (walSegmentSizeBytes <= 0L || walForceBatchSize <= 0 || walForceMaxDelayMicros < 0L
                || ringCapacity <= 0 || gatewaySpinLimit <= 0 || gatewayOfferTimeoutNanos < 0L
                || httpPort < 0 || grpcPort < 0 || binaryIngressPort < 0 || httpWorkerThreads <= 0 || httpMaxBodyBytes <= 0
                || binaryIngressMaxBatchSize <= 0
                || httpMaxConcurrentRequests <= 0 || httpRequestTimeoutMillis <= 0L
                || httpWriteMaxConcurrentRequests <= 0 || httpReadMaxConcurrentRequests <= 0 || httpAdminMaxConcurrentRequests <= 0
                || httpWriteTimeoutMillis <= 0L || httpReadTimeoutMillis <= 0L || httpAdminTimeoutMillis <= 0L
                || httpSubmitEndpointMaxConcurrentRequests <= 0 || httpCancelEndpointMaxConcurrentRequests <= 0
                || httpSnapshotEndpointMaxConcurrentRequests <= 0 || httpReadinessEndpointMaxConcurrentRequests <= 0
                || httpMetricsEndpointMaxConcurrentRequests <= 0) {
            throw new IllegalArgumentException("invalid server sizing or port configuration");
        }
    }

    public static MatcherServerConfig defaults(String nodeId, int symbolId, Path dataDirectory) {
        Path normalized = dataDirectory.toAbsolutePath().normalize();
        return new MatcherServerConfig(
                MatcherServerMode.DEV,
                nodeId,
                "symbol-" + symbolId,
                MatcherConfig.defaults(symbolId),
                normalized.resolve("wal"),
                "symbol-" + symbolId,
                64L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                normalized.resolve("snapshots").resolve("symbol-" + symbolId + ".snap"),
                1 << 16,
                10_000,
                TimeUnit.MILLISECONDS.toNanos(500),
                8080,
                "127.0.0.1",
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                1 << 20,
                256,
                2_000L,
                false,
                10080,
                "127.0.0.1",
                256,
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
                9090,
                GrpcReplicationServerConfig.defaults(9090),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                MatchLoopConfig.defaults(),
                StandbySyncConfig.defaults(),
                null
        );
    }

    public MatcherServerConfig(
            MatcherServerMode serverMode,
            String nodeId,
            String shardKey,
            MatcherConfig matcherConfig,
            Path walDirectory,
            String walPrefix,
            long walSegmentSizeBytes,
            WalDurabilityMode walDurabilityMode,
            int walForceBatchSize,
            long walForceMaxDelayMicros,
            Path snapshotFile,
            int ringCapacity,
            int gatewaySpinLimit,
            long gatewayOfferTimeoutNanos,
            int httpPort,
            String httpBindHost,
            int httpWorkerThreads,
            int httpMaxBodyBytes,
            int httpMaxConcurrentRequests,
            long httpRequestTimeoutMillis,
            int httpWriteMaxConcurrentRequests,
            int httpReadMaxConcurrentRequests,
            int httpAdminMaxConcurrentRequests,
            long httpWriteTimeoutMillis,
            long httpReadTimeoutMillis,
            long httpAdminTimeoutMillis,
            int httpSubmitEndpointMaxConcurrentRequests,
            int httpCancelEndpointMaxConcurrentRequests,
            int httpSnapshotEndpointMaxConcurrentRequests,
            int httpReadinessEndpointMaxConcurrentRequests,
            int httpMetricsEndpointMaxConcurrentRequests,
            WriteAdmissionPolicyConfig writeAdmissionPolicyConfig,
            boolean allowInsecureRemoteHttp,
            int grpcPort,
            GrpcReplicationServerConfig grpcServerConfig,
            ServerSecurityConfig securityConfig,
            TtlCancelConfig ttlCancelConfig,
            HaRole initialRole,
            MatchLoopConfig loopConfig,
            StandbySyncConfig standbySyncConfig,
            MatcherClusterConfig clusterConfig
    ) {
        this(
                serverMode,
                nodeId,
                shardKey,
                matcherConfig,
                walDirectory,
                walPrefix,
                walSegmentSizeBytes,
                walDurabilityMode,
                walForceBatchSize,
                walForceMaxDelayMicros,
                snapshotFile,
                ringCapacity,
                gatewaySpinLimit,
                gatewayOfferTimeoutNanos,
                httpPort,
                httpBindHost,
                httpWorkerThreads,
                httpMaxBodyBytes,
                httpMaxConcurrentRequests,
                httpRequestTimeoutMillis,
                false,
                10080,
                httpBindHost,
                256,
                httpWriteMaxConcurrentRequests,
                httpReadMaxConcurrentRequests,
                httpAdminMaxConcurrentRequests,
                httpWriteTimeoutMillis,
                httpReadTimeoutMillis,
                httpAdminTimeoutMillis,
                httpSubmitEndpointMaxConcurrentRequests,
                httpCancelEndpointMaxConcurrentRequests,
                httpSnapshotEndpointMaxConcurrentRequests,
                httpReadinessEndpointMaxConcurrentRequests,
                httpMetricsEndpointMaxConcurrentRequests,
                writeAdmissionPolicyConfig,
                allowInsecureRemoteHttp,
                grpcPort,
                grpcServerConfig,
                securityConfig,
                ttlCancelConfig,
                initialRole,
                loopConfig,
                standbySyncConfig,
                clusterConfig
        );
    }

    public MatcherServerConfig withClusterConfig(MatcherClusterConfig clusterConfig) {
        return new MatcherServerConfig(
                serverMode,
                nodeId,
                shardKey,
                matcherConfig,
                walDirectory,
                walPrefix,
                walSegmentSizeBytes,
                walDurabilityMode,
                walForceBatchSize,
                walForceMaxDelayMicros,
                snapshotFile,
                ringCapacity,
                gatewaySpinLimit,
                gatewayOfferTimeoutNanos,
                httpPort,
                httpBindHost,
                httpWorkerThreads,
                httpMaxBodyBytes,
                httpMaxConcurrentRequests,
                httpRequestTimeoutMillis,
                binaryIngressEnabled,
                binaryIngressPort,
                binaryIngressBindHost,
                binaryIngressMaxBatchSize,
                httpWriteMaxConcurrentRequests,
                httpReadMaxConcurrentRequests,
                httpAdminMaxConcurrentRequests,
                httpWriteTimeoutMillis,
                httpReadTimeoutMillis,
                httpAdminTimeoutMillis,
                httpSubmitEndpointMaxConcurrentRequests,
                httpCancelEndpointMaxConcurrentRequests,
                httpSnapshotEndpointMaxConcurrentRequests,
                httpReadinessEndpointMaxConcurrentRequests,
                httpMetricsEndpointMaxConcurrentRequests,
                writeAdmissionPolicyConfig,
                allowInsecureRemoteHttp,
                grpcPort,
                grpcServerConfig,
                securityConfig,
                ttlCancelConfig,
                initialRole,
                loopConfig,
                standbySyncConfig,
                clusterConfig
        );
    }

    public int httpShardWriteMaxConcurrentRequests() {
        return writeAdmissionPolicyConfig.shardMaxConcurrentRequests();
    }

    public int httpTenantWriteMaxConcurrentRequests() {
        return writeAdmissionPolicyConfig.tenantMaxConcurrentRequests();
    }

    public String httpTenantAdmissionHeader() {
        return writeAdmissionPolicyConfig.tenantAdmissionHeader();
    }

    public void validateDeploymentSafety() {
        if (serverMode != MatcherServerMode.PROD) {
            return;
        }
        if (!isLoopbackHost(httpBindHost) && !allowInsecureRemoteHttp) {
            throw new IllegalStateException("prod mode requires matcher.allowInsecureRemoteHttp=true when matcher.httpBindHost is not loopback");
        }
        if (binaryIngressEnabled && !isLoopbackHost(binaryIngressBindHost) && !allowInsecureRemoteHttp) {
            throw new IllegalStateException("prod mode requires matcher.allowInsecureRemoteHttp=true when matcher.binaryIngressBindHost is not loopback");
        }
        if (requiresGrpcReplicationServer()
                && !isLoopbackHost(grpcServerConfig.bindHost())
                && securityConfig.grpcServerTls() == null) {
            throw new IllegalStateException("prod mode requires gRPC TLS when matcher.grpcBindHost is not loopback");
        }
        if (clusterConfig != null
                && clusterConfig.replicationTransportType() == ReplicationTransportType.AERON_PREVIEW
                && !clusterConfig.replicationTransportPolicyConfig().allowPreviewTransportInProd()) {
            throw new IllegalStateException(
                    "prod mode forbids matcher.replicationTransport=AERON_PREVIEW unless matcher.allowPreviewTransportInProd=true");
        }
        if (clusterConfig != null
                && clusterConfig.replicationTransportType() == ReplicationTransportType.AERON
                && !isLoopbackHost(clusterConfig.advertisedHost())
                && !securityConfig.transportSecurityEnabled()) {
            throw new IllegalStateException("prod mode requires transport security when matcher.replicationTransport=AERON and matcher.advertisedHost is not loopback");
        }
    }

    private static boolean isLoopbackHost(String host) {
        return "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host) || "::1".equals(host);
    }

    public boolean requiresGrpcReplicationServer() {
        return clusterConfig == null || clusterConfig.replicationTransportType().requiresGrpcReplicationServer();
    }
}
