package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.runtime.MatchLoopConfig;
import io.github.ike.ullmatcher.server.api.BinaryIngressLimits;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.orchestrator.OrchestratorRegistrationConfig;
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.storage.wal.WalArchiveConfig;


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
        WalArchiveConfig walArchiveConfig,
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
        BinaryIngressLimits binaryIngressLimits,
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
        IngressAuthConfig ingressAuthConfig,
        int grpcPort,
        GrpcReplicationServerConfig grpcServerConfig,
        ServerSecurityConfig securityConfig,
        TtlCancelConfig ttlCancelConfig,
        HaRole initialRole,
        MatchLoopConfig loopConfig,
        StandbySyncConfig standbySyncConfig,
        OrchestratorRegistrationConfig orchestratorRegistrationConfig,
        MatcherClusterConfig clusterConfig
) {
    public static final WalDurabilityMode DEFAULT_WAL_DURABILITY_MODE = WalDurabilityMode.SYNC_PER_COMMAND;
    public static final int DEFAULT_WAL_FORCE_BATCH_SIZE = 1;
    public static final long DEFAULT_WAL_FORCE_MAX_DELAY_MICROS = 0L;
    public static final int DEFAULT_HTTP_MAX_CONCURRENT_REQUESTS = 2048;
    public static final int DEFAULT_HTTP_READ_MAX_CONCURRENT_REQUESTS = 1024;
    public static final int DEFAULT_HTTP_WRITE_MAX_CONCURRENT_REQUESTS = 1024;
    public static final int DEFAULT_HTTP_ADMIN_MAX_CONCURRENT_REQUESTS = 32;
    public static final int DEFAULT_HTTP_SUBMIT_ENDPOINT_MAX_CONCURRENT_REQUESTS = 512;
    public static final int DEFAULT_HTTP_CANCEL_ENDPOINT_MAX_CONCURRENT_REQUESTS = 384;
    public static final int DEFAULT_HTTP_SNAPSHOT_ENDPOINT_MAX_CONCURRENT_REQUESTS = 4;
    public static final int DEFAULT_HTTP_READINESS_ENDPOINT_MAX_CONCURRENT_REQUESTS = 64;
    public static final int DEFAULT_HTTP_METRICS_ENDPOINT_MAX_CONCURRENT_REQUESTS = 32;

    public static int defaultHttpWorkerThreads() {
        return Math.max(32, Runtime.getRuntime().availableProcessors() * 2);
    }

    /** Build output directory names that are wiped by {@code mvn clean} and must never hold prod state. */
    private static final java.util.Set<String> EPHEMERAL_PATH_SEGMENTS = java.util.Set.of("target", "build", "out");

    public MatcherServerConfig {
        Objects.requireNonNull(serverMode, "serverMode");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(shardKey, "shardKey");
        Objects.requireNonNull(matcherConfig, "matcherConfig");
        Objects.requireNonNull(walDirectory, "walDirectory");
        Objects.requireNonNull(walPrefix, "walPrefix");
        Objects.requireNonNull(snapshotFile, "snapshotFile");
        Objects.requireNonNull(walDurabilityMode, "walDurabilityMode");
        if (ingressAuthConfig == null) {
            ingressAuthConfig = IngressAuthConfig.disabled();
        }
        if (binaryIngressLimits == null) {
            binaryIngressLimits = BinaryIngressLimits.defaults();
        }
        if (walArchiveConfig == null) {
            walArchiveConfig = WalArchiveConfig.disabled();
        }
        if (orchestratorRegistrationConfig == null) {
            orchestratorRegistrationConfig = OrchestratorRegistrationConfig.disabled();
        }
        Objects.requireNonNull(initialRole, "initialRole");
        Objects.requireNonNull(grpcServerConfig, "grpcServerConfig");
        Objects.requireNonNull(securityConfig, "securityConfig");
        Objects.requireNonNull(ttlCancelConfig, "ttlCancelConfig");
        Objects.requireNonNull(loopConfig, "loopConfig");
        Objects.requireNonNull(standbySyncConfig, "standbySyncConfig");
        Objects.requireNonNull(writeAdmissionPolicyConfig, "writeAdmissionPolicyConfig");
        Objects.requireNonNull(ingressAuthConfig, "ingressAuthConfig");
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
        if (Integer.bitCount(ringCapacity) != 1) {
            throw new IllegalArgumentException("ringCapacity must be a power of two");
        }
        if (binaryIngressMaxBatchSize > ringCapacity) {
            throw new IllegalArgumentException("binaryIngressMaxBatchSize must not exceed ringCapacity");
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
                DEFAULT_WAL_DURABILITY_MODE,
                DEFAULT_WAL_FORCE_BATCH_SIZE,
                DEFAULT_WAL_FORCE_MAX_DELAY_MICROS,
                WalArchiveConfig.disabled(),
                normalized.resolve("snapshots").resolve("symbol-" + symbolId + ".snap"),
                1 << 16,
                10_000,
                TimeUnit.MILLISECONDS.toNanos(500),
                8080,
                "127.0.0.1",
                defaultHttpWorkerThreads(),
                1 << 20,
                DEFAULT_HTTP_MAX_CONCURRENT_REQUESTS,
                2_000L,
                false,
                10080,
                "127.0.0.1",
                256,
                BinaryIngressLimits.defaults(),
                DEFAULT_HTTP_WRITE_MAX_CONCURRENT_REQUESTS,
                DEFAULT_HTTP_READ_MAX_CONCURRENT_REQUESTS,
                DEFAULT_HTTP_ADMIN_MAX_CONCURRENT_REQUESTS,
                2_000L,
                1_000L,
                5_000L,
                DEFAULT_HTTP_SUBMIT_ENDPOINT_MAX_CONCURRENT_REQUESTS,
                DEFAULT_HTTP_CANCEL_ENDPOINT_MAX_CONCURRENT_REQUESTS,
                DEFAULT_HTTP_SNAPSHOT_ENDPOINT_MAX_CONCURRENT_REQUESTS,
                DEFAULT_HTTP_READINESS_ENDPOINT_MAX_CONCURRENT_REQUESTS,
                DEFAULT_HTTP_METRICS_ENDPOINT_MAX_CONCURRENT_REQUESTS,
                WriteAdmissionPolicyConfig.defaults(),
                false,
                IngressAuthConfig.disabled(),
                9090,
                GrpcReplicationServerConfig.defaults(9090),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                MatchLoopConfig.defaults(),
                StandbySyncConfig.defaults(),
                OrchestratorRegistrationConfig.disabled(),
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
            IngressAuthConfig ingressAuthConfig,
            int grpcPort,
            GrpcReplicationServerConfig grpcServerConfig,
            ServerSecurityConfig securityConfig,
            TtlCancelConfig ttlCancelConfig,
            HaRole initialRole,
            MatchLoopConfig loopConfig,
            StandbySyncConfig standbySyncConfig,
            OrchestratorRegistrationConfig orchestratorRegistrationConfig,
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
                WalArchiveConfig.disabled(),
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
                BinaryIngressLimits.defaults(),
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
                ingressAuthConfig == null ? IngressAuthConfig.disabled() : ingressAuthConfig,
                grpcPort,
                grpcServerConfig,
                securityConfig,
                ttlCancelConfig,
                initialRole,
                loopConfig,
                standbySyncConfig,
                orchestratorRegistrationConfig == null
                        ? OrchestratorRegistrationConfig.disabled()
                        : orchestratorRegistrationConfig,
                clusterConfig
        );
    }

    public MatcherServerConfig withClusterConfig(MatcherClusterConfig clusterConfig) {
        return toBuilder().clusterConfig(clusterConfig).build();
    }

    /**
     * Returns a builder seeded with {@code DEV} defaults for the given node, symbol and data directory.
     *
     * @param nodeId node identifier
     * @param symbolId symbol identifier
     * @param dataDirectory base directory for WAL and snapshots
     * @return builder seeded with defaults
     */
    public static Builder builder(String nodeId, int symbolId, Path dataDirectory) {
        return defaults(nodeId, symbolId, dataDirectory).toBuilder();
    }

    /**
     * Returns a builder seeded with every value from this configuration.
     *
     * @return builder seeded with the current values
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Mutable builder for {@link MatcherServerConfig}.
     * <p>
     * The record has enough components that positional construction is unreadable and brittle;
     * callers should start from {@link #builder(String, int, Path)} and override only what they need.
     */
    public static final class Builder {
        private MatcherServerMode serverMode;
        private String nodeId;
        private String shardKey;
        private MatcherConfig matcherConfig;
        private Path walDirectory;
        private String walPrefix;
        private long walSegmentSizeBytes;
        private WalDurabilityMode walDurabilityMode;
        private int walForceBatchSize;
        private long walForceMaxDelayMicros;
        private WalArchiveConfig walArchiveConfig;
        private Path snapshotFile;
        private int ringCapacity;
        private int gatewaySpinLimit;
        private long gatewayOfferTimeoutNanos;
        private int httpPort;
        private String httpBindHost;
        private int httpWorkerThreads;
        private int httpMaxBodyBytes;
        private int httpMaxConcurrentRequests;
        private long httpRequestTimeoutMillis;
        private boolean binaryIngressEnabled;
        private int binaryIngressPort;
        private String binaryIngressBindHost;
        private int binaryIngressMaxBatchSize;
        private BinaryIngressLimits binaryIngressLimits;
        private int httpWriteMaxConcurrentRequests;
        private int httpReadMaxConcurrentRequests;
        private int httpAdminMaxConcurrentRequests;
        private long httpWriteTimeoutMillis;
        private long httpReadTimeoutMillis;
        private long httpAdminTimeoutMillis;
        private int httpSubmitEndpointMaxConcurrentRequests;
        private int httpCancelEndpointMaxConcurrentRequests;
        private int httpSnapshotEndpointMaxConcurrentRequests;
        private int httpReadinessEndpointMaxConcurrentRequests;
        private int httpMetricsEndpointMaxConcurrentRequests;
        private WriteAdmissionPolicyConfig writeAdmissionPolicyConfig;
        private boolean allowInsecureRemoteHttp;
        private IngressAuthConfig ingressAuthConfig;
        private int grpcPort;
        private GrpcReplicationServerConfig grpcServerConfig;
        private ServerSecurityConfig securityConfig;
        private TtlCancelConfig ttlCancelConfig;
        private HaRole initialRole;
        private MatchLoopConfig loopConfig;
        private StandbySyncConfig standbySyncConfig;
        private OrchestratorRegistrationConfig orchestratorRegistrationConfig;
        private MatcherClusterConfig clusterConfig;

        private Builder(MatcherServerConfig source) {
            this.serverMode = source.serverMode;
            this.nodeId = source.nodeId;
            this.shardKey = source.shardKey;
            this.matcherConfig = source.matcherConfig;
            this.walDirectory = source.walDirectory;
            this.walPrefix = source.walPrefix;
            this.walSegmentSizeBytes = source.walSegmentSizeBytes;
            this.walDurabilityMode = source.walDurabilityMode;
            this.walForceBatchSize = source.walForceBatchSize;
            this.walForceMaxDelayMicros = source.walForceMaxDelayMicros;
            this.walArchiveConfig = source.walArchiveConfig;
            this.snapshotFile = source.snapshotFile;
            this.ringCapacity = source.ringCapacity;
            this.gatewaySpinLimit = source.gatewaySpinLimit;
            this.gatewayOfferTimeoutNanos = source.gatewayOfferTimeoutNanos;
            this.httpPort = source.httpPort;
            this.httpBindHost = source.httpBindHost;
            this.httpWorkerThreads = source.httpWorkerThreads;
            this.httpMaxBodyBytes = source.httpMaxBodyBytes;
            this.httpMaxConcurrentRequests = source.httpMaxConcurrentRequests;
            this.httpRequestTimeoutMillis = source.httpRequestTimeoutMillis;
            this.binaryIngressEnabled = source.binaryIngressEnabled;
            this.binaryIngressPort = source.binaryIngressPort;
            this.binaryIngressBindHost = source.binaryIngressBindHost;
            this.binaryIngressMaxBatchSize = source.binaryIngressMaxBatchSize;
            this.binaryIngressLimits = source.binaryIngressLimits;
            this.httpWriteMaxConcurrentRequests = source.httpWriteMaxConcurrentRequests;
            this.httpReadMaxConcurrentRequests = source.httpReadMaxConcurrentRequests;
            this.httpAdminMaxConcurrentRequests = source.httpAdminMaxConcurrentRequests;
            this.httpWriteTimeoutMillis = source.httpWriteTimeoutMillis;
            this.httpReadTimeoutMillis = source.httpReadTimeoutMillis;
            this.httpAdminTimeoutMillis = source.httpAdminTimeoutMillis;
            this.httpSubmitEndpointMaxConcurrentRequests = source.httpSubmitEndpointMaxConcurrentRequests;
            this.httpCancelEndpointMaxConcurrentRequests = source.httpCancelEndpointMaxConcurrentRequests;
            this.httpSnapshotEndpointMaxConcurrentRequests = source.httpSnapshotEndpointMaxConcurrentRequests;
            this.httpReadinessEndpointMaxConcurrentRequests = source.httpReadinessEndpointMaxConcurrentRequests;
            this.httpMetricsEndpointMaxConcurrentRequests = source.httpMetricsEndpointMaxConcurrentRequests;
            this.writeAdmissionPolicyConfig = source.writeAdmissionPolicyConfig;
            this.allowInsecureRemoteHttp = source.allowInsecureRemoteHttp;
            this.ingressAuthConfig = source.ingressAuthConfig;
            this.grpcPort = source.grpcPort;
            this.grpcServerConfig = source.grpcServerConfig;
            this.securityConfig = source.securityConfig;
            this.ttlCancelConfig = source.ttlCancelConfig;
            this.initialRole = source.initialRole;
            this.loopConfig = source.loopConfig;
            this.standbySyncConfig = source.standbySyncConfig;
            this.orchestratorRegistrationConfig = source.orchestratorRegistrationConfig;
            this.clusterConfig = source.clusterConfig;
        }

        public Builder serverMode(MatcherServerMode value) {
            this.serverMode = value;
            return this;
        }

        public Builder nodeId(String value) {
            this.nodeId = value;
            return this;
        }

        public Builder shardKey(String value) {
            this.shardKey = value;
            return this;
        }

        public Builder matcherConfig(MatcherConfig value) {
            this.matcherConfig = value;
            return this;
        }

        public Builder walDirectory(Path value) {
            this.walDirectory = value;
            return this;
        }

        public Builder walPrefix(String value) {
            this.walPrefix = value;
            return this;
        }

        public Builder walSegmentSizeBytes(long value) {
            this.walSegmentSizeBytes = value;
            return this;
        }

        public Builder walDurabilityMode(WalDurabilityMode value) {
            this.walDurabilityMode = value;
            return this;
        }

        public Builder walForceBatchSize(int value) {
            this.walForceBatchSize = value;
            return this;
        }

        public Builder walForceMaxDelayMicros(long value) {
            this.walForceMaxDelayMicros = value;
            return this;
        }

        public Builder walArchiveConfig(WalArchiveConfig value) {
            this.walArchiveConfig = value;
            return this;
        }

        public Builder snapshotFile(Path value) {
            this.snapshotFile = value;
            return this;
        }

        public Builder ringCapacity(int value) {
            this.ringCapacity = value;
            return this;
        }

        public Builder gatewaySpinLimit(int value) {
            this.gatewaySpinLimit = value;
            return this;
        }

        public Builder gatewayOfferTimeoutNanos(long value) {
            this.gatewayOfferTimeoutNanos = value;
            return this;
        }

        public Builder httpPort(int value) {
            this.httpPort = value;
            return this;
        }

        public Builder httpBindHost(String value) {
            this.httpBindHost = value;
            return this;
        }

        public Builder httpWorkerThreads(int value) {
            this.httpWorkerThreads = value;
            return this;
        }

        public Builder httpMaxBodyBytes(int value) {
            this.httpMaxBodyBytes = value;
            return this;
        }

        public Builder httpMaxConcurrentRequests(int value) {
            this.httpMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpRequestTimeoutMillis(long value) {
            this.httpRequestTimeoutMillis = value;
            return this;
        }

        public Builder binaryIngressEnabled(boolean value) {
            this.binaryIngressEnabled = value;
            return this;
        }

        public Builder binaryIngressPort(int value) {
            this.binaryIngressPort = value;
            return this;
        }

        public Builder binaryIngressBindHost(String value) {
            this.binaryIngressBindHost = value;
            return this;
        }

        public Builder binaryIngressMaxBatchSize(int value) {
            this.binaryIngressMaxBatchSize = value;
            return this;
        }

        public Builder binaryIngressLimits(BinaryIngressLimits value) {
            this.binaryIngressLimits = value;
            return this;
        }

        public Builder httpWriteMaxConcurrentRequests(int value) {
            this.httpWriteMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpReadMaxConcurrentRequests(int value) {
            this.httpReadMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpAdminMaxConcurrentRequests(int value) {
            this.httpAdminMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpWriteTimeoutMillis(long value) {
            this.httpWriteTimeoutMillis = value;
            return this;
        }

        public Builder httpReadTimeoutMillis(long value) {
            this.httpReadTimeoutMillis = value;
            return this;
        }

        public Builder httpAdminTimeoutMillis(long value) {
            this.httpAdminTimeoutMillis = value;
            return this;
        }

        public Builder httpSubmitEndpointMaxConcurrentRequests(int value) {
            this.httpSubmitEndpointMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpCancelEndpointMaxConcurrentRequests(int value) {
            this.httpCancelEndpointMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpSnapshotEndpointMaxConcurrentRequests(int value) {
            this.httpSnapshotEndpointMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpReadinessEndpointMaxConcurrentRequests(int value) {
            this.httpReadinessEndpointMaxConcurrentRequests = value;
            return this;
        }

        public Builder httpMetricsEndpointMaxConcurrentRequests(int value) {
            this.httpMetricsEndpointMaxConcurrentRequests = value;
            return this;
        }

        public Builder writeAdmissionPolicyConfig(WriteAdmissionPolicyConfig value) {
            this.writeAdmissionPolicyConfig = value;
            return this;
        }

        public Builder allowInsecureRemoteHttp(boolean value) {
            this.allowInsecureRemoteHttp = value;
            return this;
        }

        public Builder ingressAuthConfig(IngressAuthConfig value) {
            this.ingressAuthConfig = value;
            return this;
        }

        public Builder grpcPort(int value) {
            this.grpcPort = value;
            return this;
        }

        public Builder grpcServerConfig(GrpcReplicationServerConfig value) {
            this.grpcServerConfig = value;
            return this;
        }

        public Builder securityConfig(ServerSecurityConfig value) {
            this.securityConfig = value;
            return this;
        }

        public Builder ttlCancelConfig(TtlCancelConfig value) {
            this.ttlCancelConfig = value;
            return this;
        }

        public Builder initialRole(HaRole value) {
            this.initialRole = value;
            return this;
        }

        public Builder loopConfig(MatchLoopConfig value) {
            this.loopConfig = value;
            return this;
        }

        public Builder standbySyncConfig(StandbySyncConfig value) {
            this.standbySyncConfig = value;
            return this;
        }

        public Builder orchestratorRegistrationConfig(OrchestratorRegistrationConfig value) {
            this.orchestratorRegistrationConfig = value;
            return this;
        }

        public Builder clusterConfig(MatcherClusterConfig value) {
            this.clusterConfig = value;
            return this;
        }

        public MatcherServerConfig build() {
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
                    walArchiveConfig,
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
                    binaryIngressLimits,
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
                    ingressAuthConfig,
                    grpcPort,
                    grpcServerConfig,
                    securityConfig,
                    ttlCancelConfig,
                    initialRole,
                    loopConfig,
                    standbySyncConfig,
                    orchestratorRegistrationConfig,
                    clusterConfig
            );
        }
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
        if (walDurabilityMode == WalDurabilityMode.OS_BUFFERED) {
            throw new IllegalStateException("prod mode forbids matcher.walDurabilityMode=OS_BUFFERED");
        }
        if (isEphemeralBuildOutputDirectory(walDirectory)) {
            throw new IllegalStateException("prod mode requires a persistent matcher.dataDir outside build output directories; resolved wal directory="
                    + walDirectory.toAbsolutePath().normalize());
        }
        if (walArchiveConfig.enabled()) {
            Path coldDir = walArchiveConfig.coldArchiveDirectory();
            if (isEphemeralBuildOutputDirectory(coldDir)) {
                throw new IllegalStateException("prod mode requires a persistent matcher.walColdArchiveDir outside build output directories; resolved="
                        + coldDir);
            }
            if (coldDir.equals(walDirectory.toAbsolutePath().normalize())) {
                throw new IllegalStateException("matcher.walColdArchiveDir must not equal the hot WAL directory");
            }
        }
        boolean remoteHttp = !isLoopbackHost(httpBindHost);
        boolean remoteBinary = binaryIngressEnabled && !isLoopbackHost(binaryIngressBindHost);
        if ((remoteHttp || remoteBinary) && !ingressAuthConfig.enabled()) {
            throw new IllegalStateException("prod mode requires matcher.ingressApiKeys when HTTP/binary bind to non-loopback addresses");
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

    private static boolean isEphemeralBuildOutputDirectory(Path walDirectory) {
        for (Path segment : walDirectory.toAbsolutePath().normalize()) {
            if (EPHEMERAL_PATH_SEGMENTS.contains(segment.toString())) {
                return true;
            }
        }
        return false;
    }

    public boolean requiresGrpcReplicationServer() {
        return clusterConfig == null || clusterConfig.replicationTransportType().requiresGrpcReplicationServer();
    }
}
