package io.github.ike.ullmatcher.server.bootstrap;

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
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.cluster.AeronPreviewTransportConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
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
        MatcherClusterConfig clusterConfig = clusterConfig(System.getProperty("matcher.shardKey", defaults.shardKey()));
        MatcherServerConfig config = new MatcherServerConfig(
                serverMode(defaults.serverMode()),
                defaults.nodeId(),
                System.getProperty("matcher.shardKey", defaults.shardKey()),
                defaults.matcherConfig(),
                defaults.walDirectory(),
                defaults.walPrefix(),
                defaults.walSegmentSizeBytes(),
                walDurabilityMode(defaults.walDurabilityMode()),
                Integer.getInteger("matcher.walForceBatchSize", defaults.walForceBatchSize()),
                Long.getLong("matcher.walForceMaxDelayMicros", defaults.walForceMaxDelayMicros()),
                defaults.snapshotFile(),
                defaults.ringCapacity(),
                defaults.gatewaySpinLimit(),
                defaults.gatewayOfferTimeoutNanos(),
                Integer.getInteger("matcher.httpPort", defaults.httpPort()),
                System.getProperty("matcher.httpBindHost", defaults.httpBindHost()),
                defaults.httpWorkerThreads(),
                Integer.getInteger("matcher.httpMaxBodyBytes", defaults.httpMaxBodyBytes()),
                Integer.getInteger("matcher.httpMaxConcurrentRequests", defaults.httpMaxConcurrentRequests()),
                Long.getLong("matcher.httpRequestTimeoutMillis", defaults.httpRequestTimeoutMillis()),
                Boolean.getBoolean("matcher.binaryIngressEnabled"),
                Integer.getInteger("matcher.binaryIngressPort", defaults.binaryIngressPort()),
                System.getProperty("matcher.binaryIngressBindHost", defaults.binaryIngressBindHost()),
                Integer.getInteger("matcher.binaryIngressMaxBatchSize", defaults.binaryIngressMaxBatchSize()),
                Integer.getInteger("matcher.httpWriteMaxConcurrentRequests", defaults.httpWriteMaxConcurrentRequests()),
                Integer.getInteger("matcher.httpReadMaxConcurrentRequests", defaults.httpReadMaxConcurrentRequests()),
                Integer.getInteger("matcher.httpAdminMaxConcurrentRequests", defaults.httpAdminMaxConcurrentRequests()),
                Long.getLong("matcher.httpWriteTimeoutMillis", defaults.httpWriteTimeoutMillis()),
                Long.getLong("matcher.httpReadTimeoutMillis", defaults.httpReadTimeoutMillis()),
                Long.getLong("matcher.httpAdminTimeoutMillis", defaults.httpAdminTimeoutMillis()),
                Integer.getInteger("matcher.httpSubmitEndpointMaxConcurrentRequests", defaults.httpSubmitEndpointMaxConcurrentRequests()),
                Integer.getInteger("matcher.httpCancelEndpointMaxConcurrentRequests", defaults.httpCancelEndpointMaxConcurrentRequests()),
                Integer.getInteger("matcher.httpSnapshotEndpointMaxConcurrentRequests", defaults.httpSnapshotEndpointMaxConcurrentRequests()),
                Integer.getInteger("matcher.httpReadinessEndpointMaxConcurrentRequests", defaults.httpReadinessEndpointMaxConcurrentRequests()),
                Integer.getInteger("matcher.httpMetricsEndpointMaxConcurrentRequests", defaults.httpMetricsEndpointMaxConcurrentRequests()),
                writeAdmissionPolicyConfig(defaults.writeAdmissionPolicyConfig()),
                Boolean.getBoolean("matcher.allowInsecureRemoteHttp"),
                Integer.getInteger("matcher.grpcPort", defaults.grpcPort()),
                grpcServerConfig(Integer.getInteger("matcher.grpcPort", defaults.grpcPort())),
                securityConfig(),
                ttlCancelConfig(),
                initialRole(defaults.initialRole(), clusterConfig),
                defaults.loopConfig(),
                defaults.standbySyncConfig(),
                clusterConfig
        );
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

    private static WalDurabilityMode walDurabilityMode(WalDurabilityMode defaultMode) {
        return WalDurabilityMode.valueOf(System.getProperty("matcher.walDurabilityMode", defaultMode.name()).trim().toUpperCase());
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

    static MatcherClusterConfig clusterConfig(String shardKey) throws Exception {
        String zkConnect = System.getProperty("matcher.zkConnect", "");
        String etcdEndpoint = System.getProperty("matcher.etcdEndpoint", "");
        String provider = controlPlaneProvider(zkConnect, etcdEndpoint);
        if (zkConnect.isBlank() && !"etcd".equals(provider)) {
            return null;
        }
        String clusterName = System.getProperty("matcher.cluster", "default");
        String host = System.getProperty("matcher.advertisedHost", "127.0.0.1");
        LeaseStore leaseStore = leaseStore(provider, zkConnect, etcdEndpoint, clusterName);
        NodeRegistry nodeRegistry = nodeRegistry(provider, zkConnect, etcdEndpoint, clusterName);
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
                aeronPreviewTransportConfig(defaults.aeronPreviewTransportConfig()),
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

    static LeaseStore leaseStore(String provider, String zkConnect, String etcdEndpoint, String clusterName) {
        return switch (provider) {
            case "zk" -> {
                if (zkConnect.isBlank()) {
                    throw new ServerBootstrapException("matcher.zkConnect is required when matcher.leaseProvider=zk");
                }
                yield new ZooKeeperLeaseStore(
                        new ZooKeeperLeaseStoreConfig(zkConnect, "/ull-matcher/lease/" + clusterName, 15_000, 5_000)
                );
            }
            case "etcd" -> new EtcdLeaseStore(etcdConfig(etcdEndpoint, clusterName));
            default -> throw new ServerBootstrapException("unsupported lease provider: " + provider);
        };
    }

    static NodeRegistry nodeRegistry(String provider, String zkConnect, String etcdEndpoint, String clusterName) throws Exception {
        return switch (provider) {
            case "zk" -> {
                if (zkConnect.isBlank()) {
                    throw new ServerBootstrapException("matcher.zkConnect is required when matcher.discoveryProvider=zk");
                }
                yield new ZooKeeperNodeRegistry(ZooKeeperDiscoveryConfig.defaults(zkConnect, clusterName));
            }
            case "etcd" -> new EtcdNodeRegistry(etcdConfig(etcdEndpoint, clusterName));
            default -> throw new ServerBootstrapException("unsupported discovery provider: " + provider);
        };
    }

    static EtcdConfig etcdConfig(String endpoint, String clusterName) {
        if (endpoint.isBlank()) {
            throw new ServerBootstrapException("matcher.etcdEndpoint is required when using etcd provider");
        }
        EtcdConfig defaults = EtcdConfig.defaults(endpoint, clusterName);
        return new EtcdConfig(
                endpoint,
                System.getProperty("matcher.etcdKeyPrefix", defaults.keyPrefix()),
                Long.getLong("matcher.etcdLeaseTtlSeconds", defaults.leaseTtlSeconds()),
                Long.getLong("matcher.etcdTimeoutMillis", defaults.timeoutMillis()),
                Long.getLong("matcher.etcdLocalHeldCheckCacheMillis", defaults.localHeldCheckCacheMillis())
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
        Path certChain = pathProperty("matcher.transportTlsCertChain", "matcher.grpcTlsCertChain");
        Path privateKey = pathProperty("matcher.transportTlsPrivateKey", "matcher.grpcTlsPrivateKey");
        Path trustChain = pathProperty("matcher.transportTlsTrustChain", "matcher.grpcTlsTrustChain");
        boolean requireMtls = booleanProperty("matcher.transportMtlsRequired", "matcher.grpcMtlsRequired");
        long reloadIntervalMillis = longProperty("matcher.transportTlsReloadMillis", "matcher.grpcTlsReloadMillis", 0L);
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

    private static boolean booleanProperty(String preferredKey, String legacyKey) {
        String preferred = System.getProperty(preferredKey);
        if (preferred != null) {
            return Boolean.parseBoolean(preferred);
        }
        return Boolean.getBoolean(legacyKey);
    }

    private static long longProperty(String preferredKey, String legacyKey, long defaultValue) {
        String preferred = System.getProperty(preferredKey);
        if (preferred != null && !preferred.isBlank()) {
            return Long.parseLong(preferred);
        }
        return Long.getLong(legacyKey, defaultValue);
    }

    private static ReplicationTransportType transportType(ReplicationTransportType defaultValue) {
        return ReplicationTransportType.valueOf(
                System.getProperty("matcher.replicationTransport", defaultValue.name()).trim().toUpperCase()
        );
    }

    private static AeronPreviewTransportConfig aeronPreviewTransportConfig(AeronPreviewTransportConfig defaults) {
        String configuredDirectory = System.getProperty("matcher.aeronPreviewDirectory", "");
        Path directory = configuredDirectory.isBlank() ? defaults.directory() : Path.of(configuredDirectory);
        return new AeronPreviewTransportConfig(
                directory,
                Integer.getInteger("matcher.aeronPreviewPort", defaults.port()),
                Integer.getInteger("matcher.aeronPreviewStreamId", defaults.streamId())
        );
    }

    private static ReplicationTransportPolicyConfig transportPolicyConfig(ReplicationTransportPolicyConfig defaults) {
        return new ReplicationTransportPolicyConfig(
                Boolean.parseBoolean(System.getProperty("matcher.allowTransportChange",
                        Boolean.toString(defaults.allowTransportChange()))),
                System.getProperty("matcher.transportChangeWindowId", defaults.transportChangeWindowId()),
                Boolean.parseBoolean(System.getProperty("matcher.allowPreviewTransportInProd",
                        Boolean.toString(defaults.allowPreviewTransportInProd())))
        );
    }
}
