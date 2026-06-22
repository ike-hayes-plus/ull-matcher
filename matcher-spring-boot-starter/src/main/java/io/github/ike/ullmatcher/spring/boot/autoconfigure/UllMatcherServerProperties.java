package io.github.ike.ullmatcher.spring.boot.autoconfigure;

import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportType;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

@ConfigurationProperties(prefix = "ull.matcher")
public class UllMatcherServerProperties {
    private boolean enabled = true;
    private boolean autoStart = false;
    private String nodeId = "node-a";
    private String shardKey = "symbol-1";
    private int symbolId = 1;
    private String dataDir = "target/matcher-server";
    private MatcherServerMode serverMode = MatcherServerMode.DEV;
    private int httpPort = 8080;
    private String httpBindHost = "127.0.0.1";
    private int grpcPort = 9090;
    private String grpcBindHost = "127.0.0.1";
    private int httpWorkerThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
    private int httpMaxBodyBytes = 1 << 20;
    private int httpMaxConcurrentRequests = 256;
    private long httpRequestTimeoutMillis = 2_000L;
    private int httpWriteMaxConcurrentRequests = 128;
    private int httpReadMaxConcurrentRequests = 96;
    private int httpAdminMaxConcurrentRequests = 16;
    private long httpWriteTimeoutMillis = 2_000L;
    private long httpReadTimeoutMillis = 1_000L;
    private long httpAdminTimeoutMillis = 5_000L;
    private int httpSubmitEndpointMaxConcurrentRequests = 96;
    private int httpCancelEndpointMaxConcurrentRequests = 64;
    private int httpSnapshotEndpointMaxConcurrentRequests = 2;
    private int httpReadinessEndpointMaxConcurrentRequests = 16;
    private int httpMetricsEndpointMaxConcurrentRequests = 8;
    private int httpShardWriteMaxConcurrentRequests = 128;
    private int httpTenantWriteMaxConcurrentRequests = 0;
    private String httpTenantAdmissionHeader = "X-Ull-Tenant-Key";
    private double httpShardWriteRateLimitPerSecond;
    private int httpShardWriteRateBurst;
    private double httpTenantWriteRateLimitPerSecond;
    private int httpTenantWriteRateBurst;
    private int httpTenantWriteDefaultWeight = 1;
    private String httpTenantWriteWeightOverrides = "";
    private String httpTenantPriorityHeader = "X-Ull-Tenant-Priority";
    private boolean allowInsecureRemoteHttp;
    private WalDurabilityMode walDurabilityMode = WalDurabilityMode.SYNC_PER_COMMAND;
    private int walForceBatchSize = 1;
    private long walForceMaxDelayMicros;
    private final Ttl ttl = new Ttl();
    private final Tls tls = new Tls();
    private final Cluster cluster = new Cluster();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getSymbolId() {
        return symbolId;
    }

    public void setSymbolId(int symbolId) {
        this.symbolId = symbolId;
    }

    public String getShardKey() {
        return shardKey;
    }

    public void setShardKey(String shardKey) {
        this.shardKey = shardKey;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public MatcherServerMode getServerMode() {
        return serverMode;
    }

    public void setServerMode(MatcherServerMode serverMode) {
        this.serverMode = serverMode;
    }

    public String getHttpBindHost() {
        return httpBindHost;
    }

    public void setHttpBindHost(String httpBindHost) {
        this.httpBindHost = httpBindHost;
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public String getGrpcBindHost() {
        return grpcBindHost;
    }

    public void setGrpcBindHost(String grpcBindHost) {
        this.grpcBindHost = grpcBindHost;
    }

    public int getHttpWorkerThreads() {
        return httpWorkerThreads;
    }

    public void setHttpWorkerThreads(int httpWorkerThreads) {
        this.httpWorkerThreads = httpWorkerThreads;
    }

    public int getHttpMaxBodyBytes() {
        return httpMaxBodyBytes;
    }

    public void setHttpMaxBodyBytes(int httpMaxBodyBytes) {
        this.httpMaxBodyBytes = httpMaxBodyBytes;
    }

    public int getHttpMaxConcurrentRequests() {
        return httpMaxConcurrentRequests;
    }

    public void setHttpMaxConcurrentRequests(int httpMaxConcurrentRequests) {
        this.httpMaxConcurrentRequests = httpMaxConcurrentRequests;
    }

    public long getHttpRequestTimeoutMillis() {
        return httpRequestTimeoutMillis;
    }

    public void setHttpRequestTimeoutMillis(long httpRequestTimeoutMillis) {
        this.httpRequestTimeoutMillis = httpRequestTimeoutMillis;
    }

    public boolean isAllowInsecureRemoteHttp() {
        return allowInsecureRemoteHttp;
    }

    public void setAllowInsecureRemoteHttp(boolean allowInsecureRemoteHttp) {
        this.allowInsecureRemoteHttp = allowInsecureRemoteHttp;
    }

    public int getHttpWriteMaxConcurrentRequests() {
        return httpWriteMaxConcurrentRequests;
    }

    public void setHttpWriteMaxConcurrentRequests(int httpWriteMaxConcurrentRequests) {
        this.httpWriteMaxConcurrentRequests = httpWriteMaxConcurrentRequests;
    }

    public int getHttpReadMaxConcurrentRequests() {
        return httpReadMaxConcurrentRequests;
    }

    public void setHttpReadMaxConcurrentRequests(int httpReadMaxConcurrentRequests) {
        this.httpReadMaxConcurrentRequests = httpReadMaxConcurrentRequests;
    }

    public int getHttpAdminMaxConcurrentRequests() {
        return httpAdminMaxConcurrentRequests;
    }

    public void setHttpAdminMaxConcurrentRequests(int httpAdminMaxConcurrentRequests) {
        this.httpAdminMaxConcurrentRequests = httpAdminMaxConcurrentRequests;
    }

    public long getHttpWriteTimeoutMillis() {
        return httpWriteTimeoutMillis;
    }

    public void setHttpWriteTimeoutMillis(long httpWriteTimeoutMillis) {
        this.httpWriteTimeoutMillis = httpWriteTimeoutMillis;
    }

    public long getHttpReadTimeoutMillis() {
        return httpReadTimeoutMillis;
    }

    public void setHttpReadTimeoutMillis(long httpReadTimeoutMillis) {
        this.httpReadTimeoutMillis = httpReadTimeoutMillis;
    }

    public long getHttpAdminTimeoutMillis() {
        return httpAdminTimeoutMillis;
    }

    public void setHttpAdminTimeoutMillis(long httpAdminTimeoutMillis) {
        this.httpAdminTimeoutMillis = httpAdminTimeoutMillis;
    }

    public int getHttpSubmitEndpointMaxConcurrentRequests() {
        return httpSubmitEndpointMaxConcurrentRequests;
    }

    public void setHttpSubmitEndpointMaxConcurrentRequests(int httpSubmitEndpointMaxConcurrentRequests) {
        this.httpSubmitEndpointMaxConcurrentRequests = httpSubmitEndpointMaxConcurrentRequests;
    }

    public int getHttpCancelEndpointMaxConcurrentRequests() {
        return httpCancelEndpointMaxConcurrentRequests;
    }

    public void setHttpCancelEndpointMaxConcurrentRequests(int httpCancelEndpointMaxConcurrentRequests) {
        this.httpCancelEndpointMaxConcurrentRequests = httpCancelEndpointMaxConcurrentRequests;
    }

    public int getHttpSnapshotEndpointMaxConcurrentRequests() {
        return httpSnapshotEndpointMaxConcurrentRequests;
    }

    public void setHttpSnapshotEndpointMaxConcurrentRequests(int httpSnapshotEndpointMaxConcurrentRequests) {
        this.httpSnapshotEndpointMaxConcurrentRequests = httpSnapshotEndpointMaxConcurrentRequests;
    }

    public int getHttpReadinessEndpointMaxConcurrentRequests() {
        return httpReadinessEndpointMaxConcurrentRequests;
    }

    public void setHttpReadinessEndpointMaxConcurrentRequests(int httpReadinessEndpointMaxConcurrentRequests) {
        this.httpReadinessEndpointMaxConcurrentRequests = httpReadinessEndpointMaxConcurrentRequests;
    }

    public int getHttpMetricsEndpointMaxConcurrentRequests() {
        return httpMetricsEndpointMaxConcurrentRequests;
    }

    public void setHttpMetricsEndpointMaxConcurrentRequests(int httpMetricsEndpointMaxConcurrentRequests) {
        this.httpMetricsEndpointMaxConcurrentRequests = httpMetricsEndpointMaxConcurrentRequests;
    }

    public int getHttpShardWriteMaxConcurrentRequests() {
        return httpShardWriteMaxConcurrentRequests;
    }

    public void setHttpShardWriteMaxConcurrentRequests(int httpShardWriteMaxConcurrentRequests) {
        this.httpShardWriteMaxConcurrentRequests = httpShardWriteMaxConcurrentRequests;
    }

    public int getHttpTenantWriteMaxConcurrentRequests() {
        return httpTenantWriteMaxConcurrentRequests;
    }

    public void setHttpTenantWriteMaxConcurrentRequests(int httpTenantWriteMaxConcurrentRequests) {
        this.httpTenantWriteMaxConcurrentRequests = httpTenantWriteMaxConcurrentRequests;
    }

    public String getHttpTenantAdmissionHeader() {
        return httpTenantAdmissionHeader;
    }

    public void setHttpTenantAdmissionHeader(String httpTenantAdmissionHeader) {
        this.httpTenantAdmissionHeader = httpTenantAdmissionHeader;
    }

    public double getHttpShardWriteRateLimitPerSecond() {
        return httpShardWriteRateLimitPerSecond;
    }

    public void setHttpShardWriteRateLimitPerSecond(double httpShardWriteRateLimitPerSecond) {
        this.httpShardWriteRateLimitPerSecond = httpShardWriteRateLimitPerSecond;
    }

    public int getHttpShardWriteRateBurst() {
        return httpShardWriteRateBurst;
    }

    public void setHttpShardWriteRateBurst(int httpShardWriteRateBurst) {
        this.httpShardWriteRateBurst = httpShardWriteRateBurst;
    }

    public double getHttpTenantWriteRateLimitPerSecond() {
        return httpTenantWriteRateLimitPerSecond;
    }

    public void setHttpTenantWriteRateLimitPerSecond(double httpTenantWriteRateLimitPerSecond) {
        this.httpTenantWriteRateLimitPerSecond = httpTenantWriteRateLimitPerSecond;
    }

    public int getHttpTenantWriteRateBurst() {
        return httpTenantWriteRateBurst;
    }

    public void setHttpTenantWriteRateBurst(int httpTenantWriteRateBurst) {
        this.httpTenantWriteRateBurst = httpTenantWriteRateBurst;
    }

    public int getHttpTenantWriteDefaultWeight() {
        return httpTenantWriteDefaultWeight;
    }

    public void setHttpTenantWriteDefaultWeight(int httpTenantWriteDefaultWeight) {
        this.httpTenantWriteDefaultWeight = httpTenantWriteDefaultWeight;
    }

    public String getHttpTenantWriteWeightOverrides() {
        return httpTenantWriteWeightOverrides;
    }

    public void setHttpTenantWriteWeightOverrides(String httpTenantWriteWeightOverrides) {
        this.httpTenantWriteWeightOverrides = httpTenantWriteWeightOverrides;
    }

    public String getHttpTenantPriorityHeader() {
        return httpTenantPriorityHeader;
    }

    public void setHttpTenantPriorityHeader(String httpTenantPriorityHeader) {
        this.httpTenantPriorityHeader = httpTenantPriorityHeader;
    }

    public WalDurabilityMode getWalDurabilityMode() {
        return walDurabilityMode;
    }

    public void setWalDurabilityMode(WalDurabilityMode walDurabilityMode) {
        this.walDurabilityMode = walDurabilityMode;
    }

    public int getWalForceBatchSize() {
        return walForceBatchSize;
    }

    public void setWalForceBatchSize(int walForceBatchSize) {
        this.walForceBatchSize = walForceBatchSize;
    }

    public long getWalForceMaxDelayMicros() {
        return walForceMaxDelayMicros;
    }

    public void setWalForceMaxDelayMicros(long walForceMaxDelayMicros) {
        this.walForceMaxDelayMicros = walForceMaxDelayMicros;
    }

    public Ttl getTtl() {
        return ttl;
    }

    public Tls getTls() {
        return tls;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public static final class Ttl {
        private boolean enabled;
        private long sweepIntervalMillis = 1_000L;
        private long defaultMillis;
        private long hardMillis = TimeUnit.DAYS.toMillis(30);
        private long recoveredOrderMillis = TimeUnit.DAYS.toMillis(7);
        private int recentAuditLimit = 128;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getSweepIntervalMillis() {
            return sweepIntervalMillis;
        }

        public void setSweepIntervalMillis(long sweepIntervalMillis) {
            this.sweepIntervalMillis = sweepIntervalMillis;
        }

        public long getDefaultMillis() {
            return defaultMillis;
        }

        public void setDefaultMillis(long defaultMillis) {
            this.defaultMillis = defaultMillis;
        }

        public long getHardMillis() {
            return hardMillis;
        }

        public void setHardMillis(long hardMillis) {
            this.hardMillis = hardMillis;
        }

        public long getRecoveredOrderMillis() {
            return recoveredOrderMillis;
        }

        public void setRecoveredOrderMillis(long recoveredOrderMillis) {
            this.recoveredOrderMillis = recoveredOrderMillis;
        }

        public int getRecentAuditLimit() {
            return recentAuditLimit;
        }

        public void setRecentAuditLimit(int recentAuditLimit) {
            this.recentAuditLimit = recentAuditLimit;
        }
    }

    public static final class Tls {
        private String certChain;
        private String privateKey;
        private String trustChain;
        private boolean mutualTlsRequired;
        private long reloadIntervalMillis;
        private boolean openTelemetryMetricsEnabled;

        public String getCertChain() {
            return certChain;
        }

        public void setCertChain(String certChain) {
            this.certChain = certChain;
        }

        public String getPrivateKey() {
            return privateKey;
        }

        public void setPrivateKey(String privateKey) {
            this.privateKey = privateKey;
        }

        public String getTrustChain() {
            return trustChain;
        }

        public void setTrustChain(String trustChain) {
            this.trustChain = trustChain;
        }

        public boolean isMutualTlsRequired() {
            return mutualTlsRequired;
        }

        public void setMutualTlsRequired(boolean mutualTlsRequired) {
            this.mutualTlsRequired = mutualTlsRequired;
        }

        public long getReloadIntervalMillis() {
            return reloadIntervalMillis;
        }

        public void setReloadIntervalMillis(long reloadIntervalMillis) {
            this.reloadIntervalMillis = reloadIntervalMillis;
        }

        public boolean isOpenTelemetryMetricsEnabled() {
            return openTelemetryMetricsEnabled;
        }

        public void setOpenTelemetryMetricsEnabled(boolean openTelemetryMetricsEnabled) {
            this.openTelemetryMetricsEnabled = openTelemetryMetricsEnabled;
        }
    }

    public static final class Cluster {
        private boolean enabled;
        private String advertisedHost = "127.0.0.1";
        private long coordinatorTickMillis = 250L;
        private long discoveryRpcTimeoutMillis = 1_000L;
        private long leaseTtlMillis = 5_000L;
        private long snapshotSyncThreshold;
        private long snapshotSyncTimeoutMillis = 5_000L;
        private ReplicationMode replicationMode = ReplicationMode.WAIT_FOR_ANY_STANDBY;
        private long replicationTimeoutMillis = 5_000L;
        private ReplicationTransportType replicationTransport = ReplicationTransportType.GRPC;
        private final AeronPreview aeronPreview = new AeronPreview();
        private final TransportPolicy transportPolicy = new TransportPolicy();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getAdvertisedHost() {
            return advertisedHost;
        }

        public void setAdvertisedHost(String advertisedHost) {
            this.advertisedHost = advertisedHost;
        }

        public long getCoordinatorTickMillis() {
            return coordinatorTickMillis;
        }

        public void setCoordinatorTickMillis(long coordinatorTickMillis) {
            this.coordinatorTickMillis = coordinatorTickMillis;
        }

        public long getDiscoveryRpcTimeoutMillis() {
            return discoveryRpcTimeoutMillis;
        }

        public void setDiscoveryRpcTimeoutMillis(long discoveryRpcTimeoutMillis) {
            this.discoveryRpcTimeoutMillis = discoveryRpcTimeoutMillis;
        }

        public long getLeaseTtlMillis() {
            return leaseTtlMillis;
        }

        public void setLeaseTtlMillis(long leaseTtlMillis) {
            this.leaseTtlMillis = leaseTtlMillis;
        }

        public long getSnapshotSyncThreshold() {
            return snapshotSyncThreshold;
        }

        public void setSnapshotSyncThreshold(long snapshotSyncThreshold) {
            this.snapshotSyncThreshold = snapshotSyncThreshold;
        }

        public long getSnapshotSyncTimeoutMillis() {
            return snapshotSyncTimeoutMillis;
        }

        public void setSnapshotSyncTimeoutMillis(long snapshotSyncTimeoutMillis) {
            this.snapshotSyncTimeoutMillis = snapshotSyncTimeoutMillis;
        }

        public ReplicationMode getReplicationMode() {
            return replicationMode;
        }

        public void setReplicationMode(ReplicationMode replicationMode) {
            this.replicationMode = replicationMode;
        }

        public long getReplicationTimeoutMillis() {
            return replicationTimeoutMillis;
        }

        public void setReplicationTimeoutMillis(long replicationTimeoutMillis) {
            this.replicationTimeoutMillis = replicationTimeoutMillis;
        }

        public ReplicationTransportType getReplicationTransport() {
            return replicationTransport;
        }

        public void setReplicationTransport(ReplicationTransportType replicationTransport) {
            this.replicationTransport = replicationTransport;
        }

        public AeronPreview getAeronPreview() {
            return aeronPreview;
        }

        public TransportPolicy getTransportPolicy() {
            return transportPolicy;
        }
    }

    public static final class AeronPreview {
        private String directory = "target/matcher-aeron-preview";
        private int port = 15_090;
        private int streamId = 11_001;

        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getStreamId() {
            return streamId;
        }

        public void setStreamId(int streamId) {
            this.streamId = streamId;
        }
    }

    public static final class TransportPolicy {
        private boolean allowTransportChange = ReplicationTransportPolicyConfig.defaults().allowTransportChange();
        private String transportChangeWindowId = ReplicationTransportPolicyConfig.defaults().transportChangeWindowId();
        private boolean allowPreviewTransportInProd = ReplicationTransportPolicyConfig.defaults().allowPreviewTransportInProd();

        public boolean isAllowTransportChange() {
            return allowTransportChange;
        }

        public void setAllowTransportChange(boolean allowTransportChange) {
            this.allowTransportChange = allowTransportChange;
        }

        public String getTransportChangeWindowId() {
            return transportChangeWindowId;
        }

        public void setTransportChangeWindowId(String transportChangeWindowId) {
            this.transportChangeWindowId = transportChangeWindowId;
        }

        public boolean isAllowPreviewTransportInProd() {
            return allowPreviewTransportInProd;
        }

        public void setAllowPreviewTransportInProd(boolean allowPreviewTransportInProd) {
            this.allowPreviewTransportInProd = allowPreviewTransportInProd;
        }
    }
}
