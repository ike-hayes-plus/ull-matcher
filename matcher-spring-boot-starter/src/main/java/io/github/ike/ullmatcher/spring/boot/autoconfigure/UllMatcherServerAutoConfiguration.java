package io.github.ike.ullmatcher.spring.boot.autoconfigure;

import io.github.ike.ullmatcher.discovery.zookeeper.ZooKeeperDiscoveryConfig;
import io.github.ike.ullmatcher.discovery.zookeeper.ZooKeeperNodeRegistry;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.etcd.EtcdConfig;
import io.github.ike.ullmatcher.ha.etcd.EtcdLeaseStore;
import io.github.ike.ullmatcher.ha.etcd.EtcdNodeRegistry;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.zookeeper.ZooKeeperLeaseStore;
import io.github.ike.ullmatcher.ha.zookeeper.ZooKeeperLeaseStoreConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerApp;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.cluster.AeronPreviewTransportConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@AutoConfiguration
@ConditionalOnClass(MatcherServerApp.class)
@ConditionalOnProperty(prefix = "ull.matcher", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(UllMatcherServerProperties.class)
public class UllMatcherServerAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public MatcherServerConfig ullMatcherServerConfig(UllMatcherServerProperties properties,
                                                      ObjectProvider<MatcherClusterConfig> clusterConfigProvider,
                                                      ObjectProvider<ServerSecurityConfig> securityConfigProvider,
                                                      ObjectProvider<TtlCancelConfig> ttlCancelConfigProvider) {
        MatcherServerConfig defaults = MatcherServerConfig.defaults(
                properties.getNodeId(),
                properties.getSymbolId(),
                Path.of(properties.getDataDir())
        );
        return new MatcherServerConfig(
                properties.getServerMode(),
                defaults.nodeId(),
                properties.getShardKey(),
                defaults.matcherConfig(),
                defaults.walDirectory(),
                defaults.walPrefix(),
                defaults.walSegmentSizeBytes(),
                properties.getWalDurabilityMode(),
                properties.getWalForceBatchSize(),
                properties.getWalForceMaxDelayMicros(),
                defaults.snapshotFile(),
                defaults.ringCapacity(),
                defaults.gatewaySpinLimit(),
                defaults.gatewayOfferTimeoutNanos(),
                properties.getHttpPort(),
                properties.getHttpBindHost(),
                properties.getHttpWorkerThreads(),
                properties.getHttpMaxBodyBytes(),
                properties.getHttpMaxConcurrentRequests(),
                properties.getHttpRequestTimeoutMillis(),
                properties.getHttpWriteMaxConcurrentRequests(),
                properties.getHttpReadMaxConcurrentRequests(),
                properties.getHttpAdminMaxConcurrentRequests(),
                properties.getHttpWriteTimeoutMillis(),
                properties.getHttpReadTimeoutMillis(),
                properties.getHttpAdminTimeoutMillis(),
                properties.getHttpSubmitEndpointMaxConcurrentRequests(),
                properties.getHttpCancelEndpointMaxConcurrentRequests(),
                properties.getHttpSnapshotEndpointMaxConcurrentRequests(),
                properties.getHttpReadinessEndpointMaxConcurrentRequests(),
                properties.getHttpMetricsEndpointMaxConcurrentRequests(),
                new WriteAdmissionPolicyConfig(
                        properties.getHttpShardWriteMaxConcurrentRequests(),
                        properties.getHttpTenantWriteMaxConcurrentRequests(),
                        properties.getHttpTenantAdmissionHeader(),
                        properties.getHttpShardWriteRateLimitPerSecond(),
                        properties.getHttpShardWriteRateBurst(),
                        properties.getHttpTenantWriteRateLimitPerSecond(),
                        properties.getHttpTenantWriteRateBurst(),
                        properties.getHttpTenantWriteDefaultWeight(),
                        properties.getHttpTenantWriteWeightOverrides(),
                        properties.getHttpTenantPriorityHeader()
                ),
                properties.isAllowInsecureRemoteHttp(),
                properties.getGrpcPort(),
                defaults.grpcServerConfig().withBindHost(properties.getGrpcBindHost()),
                securityConfigProvider.getIfAvailable(ServerSecurityConfig::insecureDefaults),
                ttlCancelConfigProvider.getIfAvailable(TtlCancelConfig::disabled),
                defaults.initialRole(),
                defaults.loopConfig(),
                defaults.standbySyncConfig(),
                clusterConfigProvider.getIfAvailable()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public ServerSecurityConfig ullMatcherServerSecurityConfig(UllMatcherServerProperties properties) {
        UllMatcherServerProperties.Tls tls = properties.getTls();
        return ServerSecurityConfig.fromPaths(
                blankToNull(tls.getCertChain()),
                blankToNull(tls.getPrivateKey()),
                blankToNull(tls.getTrustChain()),
                tls.isMutualTlsRequired(),
                tls.getReloadIntervalMillis(),
                tls.isOpenTelemetryMetricsEnabled()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public TtlCancelConfig ullMatcherTtlCancelConfig(UllMatcherServerProperties properties) {
        UllMatcherServerProperties.Ttl ttl = properties.getTtl();
        return new TtlCancelConfig(
                ttl.isEnabled(),
                ttl.getSweepIntervalMillis(),
                ttl.getDefaultMillis(),
                ttl.getHardMillis(),
                ttl.getRecoveredOrderMillis(),
                ttl.getRecentAuditLimit()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "ull.matcher.cluster", name = "enabled", havingValue = "true")
    public LeaseStore ullMatcherLeaseStore(UllMatcherServerProperties properties) {
        UllMatcherServerProperties.Cluster cluster = properties.getCluster();
        return switch (controlPlaneProvider(cluster)) {
            case "zk" -> new ZooKeeperLeaseStore(
                    new ZooKeeperLeaseStoreConfig(
                            required(cluster.getZookeeperConnect(), "ull.matcher.cluster.zookeeper-connect"),
                            "/ull-matcher/lease/" + cluster.getName(),
                            cluster.getZookeeperSessionTimeoutMillis(),
                            cluster.getZookeeperConnectionTimeoutMillis()
                    )
            );
            case "etcd" -> new EtcdLeaseStore(etcdConfig(cluster));
            default -> throw new IllegalArgumentException("unsupported ull.matcher.cluster.lease-provider: "
                    + cluster.getLeaseProvider());
        };
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "ull.matcher.cluster", name = "enabled", havingValue = "true")
    public NodeRegistry ullMatcherNodeRegistry(UllMatcherServerProperties properties) {
        UllMatcherServerProperties.Cluster cluster = properties.getCluster();
        return switch (controlPlaneProvider(cluster)) {
            case "zk" -> new ZooKeeperNodeRegistry(
                    new ZooKeeperDiscoveryConfig(
                            required(cluster.getZookeeperConnect(), "ull.matcher.cluster.zookeeper-connect"),
                            "/ull-matcher/discovery/" + cluster.getName() + "/nodes",
                            cluster.getZookeeperSessionTimeoutMillis(),
                            cluster.getZookeeperConnectionTimeoutMillis()
                    )
            );
            case "etcd" -> new EtcdNodeRegistry(etcdConfig(cluster));
            default -> throw new IllegalArgumentException("unsupported ull.matcher.cluster.discovery-provider: "
                    + cluster.getDiscoveryProvider());
        };
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "ull.matcher.cluster", name = "enabled", havingValue = "true")
    public MatcherClusterConfig ullMatcherClusterConfig(UllMatcherServerProperties properties,
                                                        LeaseStore leaseStore,
                                                        NodeRegistry nodeRegistry) {
        UllMatcherServerProperties.Cluster cluster = properties.getCluster();
        UllMatcherServerProperties.AeronPreview aeronPreview = cluster.getAeronPreview();
        UllMatcherServerProperties.TransportPolicy transportPolicy = cluster.getTransportPolicy();
        return new MatcherClusterConfig(
                leaseStore,
                nodeRegistry,
                properties.getShardKey(),
                cluster.getAdvertisedHost(),
                cluster.getCoordinatorTickMillis(),
                TimeUnit.MILLISECONDS.toNanos(cluster.getDiscoveryRpcTimeoutMillis()),
                TimeUnit.MILLISECONDS.toNanos(cluster.getLeaseTtlMillis()),
                new FailoverPolicy(
                        TimeUnit.MILLISECONDS.toNanos(cluster.getFailoverPrimaryHeartbeatTimeoutMillis()),
                        cluster.getFailoverMaxPromotionLag(),
                        cluster.getFailoverMinStandbyReplicas()
                ),
                MatcherClusterConfig.defaults(leaseStore, nodeRegistry, cluster.getAdvertisedHost(), properties.getShardKey()).readinessPolicy(),
                cluster.getSnapshotSyncThreshold(),
                TimeUnit.MILLISECONDS.toNanos(cluster.getSnapshotSyncTimeoutMillis()),
                cluster.getReplicationMode(),
                TimeUnit.MILLISECONDS.toNanos(cluster.getReplicationTimeoutMillis()),
                cluster.getReplicationTransport(),
                new AeronPreviewTransportConfig(
                        Path.of(aeronPreview.getDirectory()),
                        aeronPreview.getPort(),
                        aeronPreview.getStreamId()
                ),
                new ReplicationTransportPolicyConfig(
                        transportPolicy.isAllowTransportChange(),
                        transportPolicy.getTransportChangeWindowId(),
                        transportPolicy.isAllowPreviewTransportInProd()
                )
        );
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "ull.matcher", name = "auto-start", havingValue = "true")
    public MatcherServerApp ullMatcherServerApp(MatcherServerConfig config) throws IOException {
        return new MatcherServerApp(config);
    }

    private static Path blankToNull(String path) {
        return path == null || path.isBlank() ? null : Path.of(path);
    }

    private static EtcdConfig etcdConfig(UllMatcherServerProperties.Cluster cluster) {
        return new EtcdConfig(
                required(cluster.getEtcdEndpoint(), "ull.matcher.cluster.etcd-endpoint"),
                cluster.getEtcdKeyPrefix().isBlank() ? "/ull-matcher/" + cluster.getName() : cluster.getEtcdKeyPrefix(),
                cluster.getEtcdLeaseTtlSeconds(),
                cluster.getEtcdTimeoutMillis(),
                cluster.getEtcdLocalHeldCheckCacheMillis()
        );
    }

    private static String controlPlaneProvider(UllMatcherServerProperties.Cluster cluster) {
        String inferred = cluster.getZookeeperConnect().isBlank() && !cluster.getEtcdEndpoint().isBlank() ? "etcd" : "zk";
        String leaseProvider = configuredProviderOrDefault(cluster.getLeaseProvider(), inferred);
        String discoveryProvider = configuredProviderOrDefault(cluster.getDiscoveryProvider(), leaseProvider);
        if (!leaseProvider.equals(discoveryProvider)) {
            throw new IllegalArgumentException("ull.matcher.cluster.lease-provider and "
                    + "ull.matcher.cluster.discovery-provider must match: "
                    + leaseProvider + " != " + discoveryProvider);
        }
        return leaseProvider;
    }

    private static String configuredProviderOrDefault(String configuredProvider, String defaultProvider) {
        if (configuredProvider != null && !configuredProvider.isBlank()) {
            return configuredProvider;
        }
        return defaultProvider;
    }

    private static String required(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(propertyName + " is required");
        }
        return value;
    }
}
