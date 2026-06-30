package io.github.ike.ullmatcher.spring.boot.autoconfigure;

import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
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
}
