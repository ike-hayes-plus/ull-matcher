package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.discovery.zookeeper.ZooKeeperDiscoveryConfig;
import io.github.ike.ullmatcher.discovery.zookeeper.ZooKeeperNodeRegistry;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.etcd.EtcdConfig;
import io.github.ike.ullmatcher.ha.etcd.EtcdLeaseStore;
import io.github.ike.ullmatcher.ha.etcd.EtcdNodeRegistry;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.zookeeper.ZooKeeperLeaseStore;
import io.github.ike.ullmatcher.ha.zookeeper.ZooKeeperLeaseStoreConfig;
import io.github.ike.ullmatcher.server.cluster.AeronTransportConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.orchestrator.OrchestratorRegistrationConfig;
import io.github.ike.ullmatcher.server.api.BinaryIngressLimits;
import io.github.ike.ullmatcher.server.api.HttpSubmitAckMode;
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.storage.wal.WalArchiveConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public final class MatcherServerMain {
    private static final Logger LOG = LoggerFactory.getLogger(MatcherServerMain.class);

    private MatcherServerMain() {}

    public static void main(String[] args) throws Exception {
        String nodeId = System.getProperty("matcher.nodeId", "node-a");
        int symbolId = Integer.getInteger("matcher.symbolId", 1);
        Path dataDir = Path.of(System.getProperty("matcher.dataDir", "target/matcher-server"));
        MatcherServerConfig defaults = MatcherServerConfig.defaults(nodeId, symbolId, dataDir);
        MatcherServerMode serverMode = serverMode(defaults.serverMode());
        MatcherClusterConfig clusterConfig =
                clusterConfig(System.getProperty("matcher.shardKey", defaults.shardKey()), serverMode);
        MatcherServerConfig.Builder builder = defaults.toBuilder()
                .serverMode(serverMode)
                .shardKey(System.getProperty("matcher.shardKey", defaults.shardKey()))
                .matcherConfig(matcherConfig(defaults.matcherConfig()))
                .walArchiveConfig(WalArchiveConfig.fromProperty(System.getProperty("matcher.walColdArchiveDir")))
                .httpPort(Integer.getInteger("matcher.httpPort", defaults.httpPort()))
                .httpBindHost(System.getProperty("matcher.httpBindHost", defaults.httpBindHost()))
                .httpMaxBodyBytes(Integer.getInteger("matcher.httpMaxBodyBytes", defaults.httpMaxBodyBytes()))
                .httpWorkerThreads(Integer.getInteger("matcher.httpWorkerThreads", defaults.httpWorkerThreads()))
                .httpMaxConcurrentRequests(Integer.getInteger("matcher.httpMaxConcurrentRequests", defaults.httpMaxConcurrentRequests()))
                .httpRequestTimeoutMillis(Long.getLong("matcher.httpRequestTimeoutMillis", defaults.httpRequestTimeoutMillis()))
                .binaryIngressEnabled(Boolean.getBoolean("matcher.binaryIngressEnabled"))
                .binaryIngressPort(Integer.getInteger("matcher.binaryIngressPort", defaults.binaryIngressPort()))
                .binaryIngressBindHost(System.getProperty("matcher.binaryIngressBindHost", defaults.binaryIngressBindHost()))
                .binaryIngressMaxBatchSize(Integer.getInteger("matcher.binaryIngressMaxBatchSize", defaults.binaryIngressMaxBatchSize()))
                .binaryIngressLimits(binaryIngressLimits(defaults.binaryIngressLimits()))
                .httpWriteMaxConcurrentRequests(Integer.getInteger("matcher.httpWriteMaxConcurrentRequests", defaults.httpWriteMaxConcurrentRequests()))
                .httpReadMaxConcurrentRequests(Integer.getInteger("matcher.httpReadMaxConcurrentRequests", defaults.httpReadMaxConcurrentRequests()))
                .httpAdminMaxConcurrentRequests(Integer.getInteger("matcher.httpAdminMaxConcurrentRequests", defaults.httpAdminMaxConcurrentRequests()))
                .httpWriteTimeoutMillis(Long.getLong("matcher.httpWriteTimeoutMillis", defaults.httpWriteTimeoutMillis()))
                .httpReadTimeoutMillis(Long.getLong("matcher.httpReadTimeoutMillis", defaults.httpReadTimeoutMillis()))
                .httpAdminTimeoutMillis(Long.getLong("matcher.httpAdminTimeoutMillis", defaults.httpAdminTimeoutMillis()))
                .httpSubmitEndpointMaxConcurrentRequests(Integer.getInteger("matcher.httpSubmitEndpointMaxConcurrentRequests", defaults.httpSubmitEndpointMaxConcurrentRequests()))
                .httpCancelEndpointMaxConcurrentRequests(Integer.getInteger("matcher.httpCancelEndpointMaxConcurrentRequests", defaults.httpCancelEndpointMaxConcurrentRequests()))
                .httpSnapshotEndpointMaxConcurrentRequests(Integer.getInteger("matcher.httpSnapshotEndpointMaxConcurrentRequests", defaults.httpSnapshotEndpointMaxConcurrentRequests()))
                .httpReadinessEndpointMaxConcurrentRequests(Integer.getInteger("matcher.httpReadinessEndpointMaxConcurrentRequests", defaults.httpReadinessEndpointMaxConcurrentRequests()))
                .httpMetricsEndpointMaxConcurrentRequests(Integer.getInteger("matcher.httpMetricsEndpointMaxConcurrentRequests", defaults.httpMetricsEndpointMaxConcurrentRequests()))
                .writeAdmissionPolicyConfig(writeAdmissionPolicyConfig(defaults.writeAdmissionPolicyConfig()))
                .allowInsecureRemoteHttp(Boolean.getBoolean("matcher.allowInsecureRemoteHttp"))
                .ingressAuthConfig(ingressAuthConfig())
                .grpcPort(Integer.getInteger("matcher.grpcPort", defaults.grpcPort()))
                .grpcServerConfig(grpcServerConfig(Integer.getInteger("matcher.grpcPort", defaults.grpcPort())))
                .securityConfig(securityConfig())
                .ttlCancelConfig(ttlCancelConfig())
                .initialRole(initialRole(defaults.initialRole(), clusterConfig))
                .orchestratorRegistrationConfig(orchestratorRegistrationConfig(serverMode, clusterConfig))
                .clusterConfig(clusterConfig)
                .httpSubmitAckMode(HttpSubmitAckMode.parse(
                        System.getProperty("matcher.httpSubmitAckMode"), HttpSubmitAckMode.LOCAL));
        PersistenceSettings.apply(builder, serverMode, defaults);
        MatcherServerConfig config = builder.build();
        MatcherServerApp app = new MatcherServerApp(config);
        app.start();
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            try {
                app.close();
            } catch (Exception e) {
                LOG.warn("failed to close matcher server cleanly", e);
            }
        }));
        Thread.currentThread().join();
    }

    /**
     * 集群模式下节点统一从待命角色启动，由租约与切换控制面决定首个主节点。
     */
    private static HaRole initialRole(HaRole standaloneDefault, MatcherClusterConfig clusterConfig) {
        return clusterConfig == null ? standaloneDefault : HaRole.STANDBY;
    }

    private static MatcherServerMode serverMode(MatcherServerMode defaultMode) {
        return MatcherServerMode.valueOf(System.getProperty("matcher.serverMode", defaultMode.name()).trim().toUpperCase());
    }

    static MatcherConfig matcherConfig(MatcherConfig defaults) {
        return new MatcherConfig(
                defaults.symbolId(),
                Integer.getInteger("matcher.expectedPriceLevels", defaults.expectedPriceLevels()),
                Integer.getInteger("matcher.expectedLiveOrders", defaults.expectedLiveOrders()),
                Integer.getInteger("matcher.orderPoolSize", defaults.orderPoolSize()),
                Long.getLong("matcher.quoteScale", defaults.quoteScale()),
                Boolean.parseBoolean(System.getProperty(
                        "matcher.preventSelfTrade",
                        Boolean.toString(defaults.preventSelfTrade())
                ))
        );
    }

    private static ReplicationMode replicationMode(ReplicationMode defaultMode) {
        return ReplicationMode.valueOf(System.getProperty("matcher.replicationMode", defaultMode.name()).trim().toUpperCase());
    }

    private static FailoverPolicy failoverPolicy(FailoverPolicy defaults) {
        return new FailoverPolicy(
                TimeUnit.MILLISECONDS.toNanos(Long.getLong(
                        "matcher.failoverPrimaryHeartbeatTimeoutMillis",
                        TimeUnit.NANOSECONDS.toMillis(defaults.primaryHeartbeatTimeoutNanos())
                )),
                Long.getLong("matcher.failoverMaxPromotionLag", defaults.maxPromotionLag()),
                Integer.getInteger("matcher.failoverMinStandbyReplicas", defaults.minStandbyReplicas())
        );
    }

    private static WriteAdmissionPolicyConfig writeAdmissionPolicyConfig(WriteAdmissionPolicyConfig defaults) {
        return new WriteAdmissionPolicyConfig(
                Integer.getInteger("matcher.httpShardWriteMaxConcurrentRequests", defaults.shardMaxConcurrentRequests()),
                Integer.getInteger("matcher.httpTenantWriteMaxConcurrentRequests", defaults.tenantMaxConcurrentRequests()),
                System.getProperty("matcher.httpTenantAdmissionHeader", defaults.tenantAdmissionHeader()),
                doubleProperty("matcher.httpShardWriteRateLimitPerSecond", defaults.shardRateLimitPerSecond()),
                Integer.getInteger("matcher.httpShardWriteRateBurst", defaults.shardRateBurst()),
                doubleProperty("matcher.httpTenantWriteRateLimitPerSecond", defaults.tenantRateLimitPerSecond()),
                Integer.getInteger("matcher.httpTenantWriteRateBurst", defaults.tenantRateBurst()),
                Integer.getInteger("matcher.httpTenantWriteDefaultWeight", defaults.tenantDefaultWeight()),
                System.getProperty("matcher.httpTenantWriteWeightOverrides", defaults.tenantWeightOverrides()),
                System.getProperty("matcher.httpTenantPriorityHeader", defaults.tenantPriorityHeader())
        );
    }

    static MatcherClusterConfig clusterConfig(String shardKey, MatcherServerMode serverMode) throws Exception {
        String zkConnect = System.getProperty("matcher.zkConnect", "");
        String etcdEndpoint = System.getProperty("matcher.etcdEndpoint", "");
        String provider = controlPlaneProvider(zkConnect, etcdEndpoint);
        if (zkConnect.isBlank() && !"etcd".equals(provider)) {
            return null;
        }
        String clusterName = cluster();
        String host = System.getProperty("matcher.advertisedHost", "127.0.0.1");
        boolean prod = serverMode == MatcherServerMode.PROD;
        LeaseStore leaseStore = leaseStore(provider, zkConnect, etcdEndpoint, clusterName, prod);
        NodeRegistry nodeRegistry = nodeRegistry(provider, zkConnect, etcdEndpoint, clusterName, prod);
        LOG.info("cluster control plane provider={} advertisedHost={}", provider, host);
        MatcherClusterConfig defaults = MatcherClusterConfig.defaults(leaseStore, nodeRegistry, host, shardKey);
        return new MatcherClusterConfig(
                defaults.leaseStore(),
                defaults.nodeRegistry(),
                defaults.shardKey(),
                defaults.advertisedHost(),
                Long.getLong("matcher.coordinatorTickMillis", defaults.coordinatorTickMillis()),
                defaults.discoveryRpcTimeoutNanos(),
                defaults.leaseTtlNanos(),
                failoverPolicy(defaults.failoverPolicy()),
                defaults.readinessPolicy(),
                Long.getLong("matcher.snapshotSyncThreshold", defaults.snapshotSyncThreshold()),
                defaults.snapshotSyncTimeoutNanos(),
                replicationMode(defaults.replicationMode()),
                TimeUnit.MILLISECONDS.toNanos(
                        Long.getLong(
                                "matcher.replicationTimeoutMillis",
                                TimeUnit.NANOSECONDS.toMillis(defaults.replicationTimeoutNanos())
                        )
                ),
                transportType(defaults.replicationTransportType()),
                aeronTransportConfig(defaults.aeronTransportConfig()),
                transportPolicyConfig(defaults.replicationTransportPolicyConfig())
        );
    }

    static String controlPlaneProvider(String zkConnect, String etcdEndpoint) {
        String defaultProvider = zkConnect.isBlank() && !etcdEndpoint.isBlank() ? "etcd" : "zk";
        String leaseProvider = System.getProperty("matcher.leaseProvider", defaultProvider);
        String discoveryProvider = System.getProperty("matcher.discoveryProvider", leaseProvider);
        if (!leaseProvider.equals(discoveryProvider)) {
            throw new ServerBootstrapException("matcher.leaseProvider and matcher.discoveryProvider must match: "
                    + leaseProvider + " != " + discoveryProvider);
        }
        return leaseProvider;
    }

    static LeaseStore leaseStore(String provider,
                                 String zkConnect,
                                 String etcdEndpoint,
                                 String clusterName,
                                 boolean enforceProductionSafety) throws Exception {
        return switch (provider) {
            case "zk" -> {
                if (zkConnect.isBlank()) {
                    throw new ServerBootstrapException("matcher.zkConnect is required when matcher.leaseProvider=zk");
                }
                ZooKeeperLeaseStoreConfig zkConfig = new ZooKeeperLeaseStoreConfig(
                        zkConnect, "/ull-matcher/lease/" + clusterName, 15_000, 5_000);
                if (enforceProductionSafety) {
                    try {
                        zkConfig.validateProductionSafety();
                    } catch (IllegalStateException e) {
                        throw new ServerBootstrapException(e.getMessage());
                    }
                }
                yield new ZooKeeperLeaseStore(zkConfig);
            }
            case "etcd" -> new EtcdLeaseStore(etcdConfig(etcdEndpoint, clusterName, enforceProductionSafety));
            default -> throw new ServerBootstrapException("unsupported lease provider: " + provider);
        };
    }

    static NodeRegistry nodeRegistry(String provider,
                                     String zkConnect,
                                     String etcdEndpoint,
                                     String clusterName,
                                     boolean enforceProductionSafety) throws Exception {
        return switch (provider) {
            case "zk" -> {
                if (zkConnect.isBlank()) {
                    throw new ServerBootstrapException("matcher.zkConnect is required when matcher.discoveryProvider=zk");
                }
                ZooKeeperDiscoveryConfig zkDiscovery = ZooKeeperDiscoveryConfig.defaults(zkConnect, clusterName);
                if (enforceProductionSafety) {
                    try {
                        zkDiscovery.validateProductionSafety();
                    } catch (IllegalStateException e) {
                        throw new ServerBootstrapException(e.getMessage());
                    }
                }
                yield new ZooKeeperNodeRegistry(zkDiscovery);
            }
            case "etcd" -> new EtcdNodeRegistry(etcdConfig(etcdEndpoint, clusterName, enforceProductionSafety));
            default -> throw new ServerBootstrapException("unsupported discovery provider: " + provider);
        };
    }

    static String cluster() {
        String configured = System.getProperty("matcher.cluster");
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        return "default";
    }

    static OrchestratorRegistrationConfig orchestratorRegistrationConfig(MatcherServerMode serverMode,
                                                                           MatcherClusterConfig clusterConfig) {
        if (!Boolean.getBoolean("matcher.orchestratorEnabled")) {
            return OrchestratorRegistrationConfig.disabled();
        }
        if (clusterConfig == null) {
            throw new ServerBootstrapException(
                    "matcher.orchestratorEnabled requires HA cluster mode (etcd control plane)");
        }
        String etcdEndpoint = System.getProperty("matcher.etcdEndpoint", "");
        String provider = controlPlaneProvider(System.getProperty("matcher.zkConnect", ""), etcdEndpoint);
        if (!"etcd".equals(provider)) {
            throw new ServerBootstrapException("matcher.orchestratorEnabled requires matcher control plane provider etcd");
        }
        boolean prod = serverMode == MatcherServerMode.PROD;
        long generation = Long.getLong("matcher.orchestratorGeneration", 1L);
        return OrchestratorRegistrationConfig.etcd(generation, etcdConfig(etcdEndpoint, cluster(), prod));
    }

    static EtcdConfig etcdConfig(String endpoint, String clusterName, boolean enforceProductionSafety) {
        if (endpoint.isBlank()) {
            throw new ServerBootstrapException("matcher.etcdEndpoint is required when using etcd provider");
        }
        EtcdConfig defaults = EtcdConfig.defaults(endpoint, clusterName);
        return new EtcdConfig(
                endpoint,
                System.getProperty("matcher.etcdKeyPrefix", defaults.keyPrefix()),
                Long.getLong("matcher.etcdLeaseTtlSeconds", defaults.leaseTtlSeconds()),
                Long.getLong("matcher.etcdTimeoutMillis", defaults.timeoutMillis()),
                pathProperty("matcher.etcdTlsTrustChain"),
                pathProperty("matcher.etcdTlsCertChain"),
                pathProperty("matcher.etcdTlsPrivateKey"),
                enforceProductionSafety
        );
    }

    private static BinaryIngressLimits binaryIngressLimits(BinaryIngressLimits defaults) {
        return new BinaryIngressLimits(
                Integer.getInteger("matcher.binaryIngressMaxConnections", defaults.maxConnections()),
                Long.getLong("matcher.binaryIngressHandshakeTimeoutMillis", defaults.handshakeTimeoutMillis()),
                Long.getLong("matcher.binaryIngressIdleTimeoutMillis", defaults.idleTimeoutMillis())
        );
    }

    private static IngressAuthConfig ingressAuthConfig() {
        return IngressAuthConfig.fromCommaSeparated(
                System.getProperty("matcher.ingressApiKeys", ""),
                System.getProperty("matcher.ingressApiKeyHeader", IngressAuthConfig.DEFAULT_API_KEY_HEADER)
        );
    }

    private static GrpcReplicationServerConfig grpcServerConfig(int grpcPort) {
        ServerSecurityConfig securityConfig = securityConfig();
        return new GrpcReplicationServerConfig(
                System.getProperty("matcher.grpcBindHost", "127.0.0.1"),
                grpcPort,
                Integer.getInteger("matcher.grpcMaxInboundBytes", 4 << 20),
                Long.getLong("matcher.grpcPermitKeepAliveSeconds", 30L),
                Long.getLong("matcher.grpcReplicationIngressTimeoutMillis", 2_000L),
                System.getProperty("matcher.grpcCompression", "identity"),
                securityConfig.grpcServerTls()
        );
    }

    private static ServerSecurityConfig securityConfig() {
        Path certChain = pathProperty("matcher.transportTlsCertChain");
        Path privateKey = pathProperty("matcher.transportTlsPrivateKey");
        Path trustChain = pathProperty("matcher.transportTlsTrustChain");
        boolean requireMtls = Boolean.getBoolean("matcher.transportMtlsRequired");
        long reloadIntervalMillis = Long.getLong("matcher.transportTlsReloadMillis", 0L);
        boolean enableOtelMetrics = Boolean.getBoolean("matcher.otelMetricsEnabled");
        return ServerSecurityConfig.fromPaths(certChain, privateKey, trustChain, requireMtls, reloadIntervalMillis, enableOtelMetrics);
    }

    private static TtlCancelConfig ttlCancelConfig() {
        TtlCancelConfig defaults = TtlCancelConfig.defaults();
        return new TtlCancelConfig(
                Boolean.getBoolean("matcher.ttlCancelEnabled"),
                Long.getLong("matcher.ttlSweepIntervalMillis", defaults.sweepIntervalMillis()),
                Long.getLong("matcher.ttlDefaultMillis", defaults.defaultTtlMillis()),
                Long.getLong("matcher.ttlHardMillis", defaults.hardTtlMillis()),
                Long.getLong("matcher.ttlRecoveredOrderMillis", defaults.recoveredOrderTtlMillis()),
                Integer.getInteger("matcher.ttlRecentAuditLimit", defaults.recentAuditLimit())
        );
    }

    private static Path pathProperty(String... keys) {
        for (String key : keys) {
            String value = System.getProperty(key, "");
            if (!value.isBlank()) {
                return Path.of(value);
            }
        }
        return null;
    }

    private static double doubleProperty(String key, double defaultValue) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : Double.parseDouble(value);
    }

    private static ReplicationTransportType transportType(ReplicationTransportType defaultValue) {
        return ReplicationTransportType.valueOf(
                System.getProperty("matcher.replicationTransport", defaultValue.name()).trim().toUpperCase()
        );
    }

    private static AeronTransportConfig aeronTransportConfig(AeronTransportConfig defaults) {
        String configuredDirectory = System.getProperty("matcher.aeronDirectory", "");
        Path directory = configuredDirectory.isBlank() ? defaults.directory() : Path.of(configuredDirectory);
        return new AeronTransportConfig(
                directory,
                Integer.getInteger("matcher.aeronPort", defaults.port()),
                Integer.getInteger("matcher.aeronStreamId", defaults.streamId())
        );
    }

    private static ReplicationTransportPolicyConfig transportPolicyConfig(ReplicationTransportPolicyConfig defaults) {
        return new ReplicationTransportPolicyConfig(
                Boolean.parseBoolean(System.getProperty("matcher.allowTransportChange",
                        Boolean.toString(defaults.allowTransportChange()))),
                System.getProperty("matcher.transportChangeWindowId", defaults.transportChangeWindowId())
        );
    }
}
