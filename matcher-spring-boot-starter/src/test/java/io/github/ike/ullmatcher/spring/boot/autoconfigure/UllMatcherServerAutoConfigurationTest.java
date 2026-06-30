package io.github.ike.ullmatcher.spring.boot.autoconfigure;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportType;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UllMatcherServerAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(UllMatcherServerAutoConfiguration.class));

    @Test
    void bindsWalTlsAndTtlProperties() {
        contextRunner
                .withPropertyValues(
                        "ull.matcher.node-id=node-b",
                        "ull.matcher.shard-key=merchant:42",
                        "ull.matcher.symbol-id=7",
                        "ull.matcher.server-mode=PROD",
                        "ull.matcher.http-port=18080",
                        "ull.matcher.http-bind-host=127.0.0.1",
                        "ull.matcher.grpc-port=19090",
                        "ull.matcher.grpc-bind-host=127.0.0.1",
                        "ull.matcher.http-worker-threads=6",
                        "ull.matcher.http-max-body-bytes=4096",
                        "ull.matcher.http-max-concurrent-requests=128",
                        "ull.matcher.http-request-timeout-millis=1500",
                        "ull.matcher.http-write-max-concurrent-requests=64",
                        "ull.matcher.http-read-max-concurrent-requests=48",
                        "ull.matcher.http-admin-max-concurrent-requests=8",
                        "ull.matcher.http-write-timeout-millis=2100",
                        "ull.matcher.http-read-timeout-millis=1200",
                        "ull.matcher.http-admin-timeout-millis=5500",
                        "ull.matcher.http-submit-endpoint-max-concurrent-requests=40",
                        "ull.matcher.http-cancel-endpoint-max-concurrent-requests=24",
                        "ull.matcher.http-snapshot-endpoint-max-concurrent-requests=3",
                        "ull.matcher.http-readiness-endpoint-max-concurrent-requests=12",
                        "ull.matcher.http-metrics-endpoint-max-concurrent-requests=6",
                        "ull.matcher.http-shard-write-max-concurrent-requests=50",
                        "ull.matcher.http-tenant-write-max-concurrent-requests=7",
                        "ull.matcher.http-tenant-admission-header=X-Tenant-Key",
                        "ull.matcher.http-shard-write-rate-limit-per-second=120.5",
                        "ull.matcher.http-shard-write-rate-burst=24",
                        "ull.matcher.http-tenant-write-rate-limit-per-second=40.0",
                        "ull.matcher.http-tenant-write-rate-burst=8",
                        "ull.matcher.http-tenant-write-default-weight=2",
                        "ull.matcher.http-tenant-write-weight-overrides=vip-a=5,vip-b=3",
                        "ull.matcher.http-tenant-priority-header=X-Tenant-Priority",
                        "ull.matcher.wal-durability-mode=SYNC_PER_BATCH",
                        "ull.matcher.wal-force-batch-size=32",
                        "ull.matcher.ttl.enabled=true",
                        "ull.matcher.ttl.sweep-interval-millis=250",
                        "ull.matcher.ttl.default-millis=1000",
                        "ull.matcher.ttl.hard-millis=2000",
                        "ull.matcher.ttl.recovered-order-millis=3000",
                        "ull.matcher.ttl.recent-audit-limit=16",
                        "ull.matcher.tls.cert-chain=/tmp/server.crt",
                        "ull.matcher.tls.private-key=/tmp/server.key",
                        "ull.matcher.tls.trust-chain=/tmp/ca.crt",
                        "ull.matcher.tls.mutual-tls-required=true",
                        "ull.matcher.tls.reload-interval-millis=5000",
                        "ull.matcher.tls.open-telemetry-metrics-enabled=true"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(MatcherServerConfig.class);
                    assertThat(context).hasSingleBean(ServerSecurityConfig.class);
                    assertThat(context).hasSingleBean(TtlCancelConfig.class);

                    MatcherServerConfig config = context.getBean(MatcherServerConfig.class);
                    ServerSecurityConfig securityConfig = context.getBean(ServerSecurityConfig.class);
                    TtlCancelConfig ttlConfig = context.getBean(TtlCancelConfig.class);

                    assertThat(config.nodeId()).isEqualTo("node-b");
                    assertThat(config.shardKey()).isEqualTo("merchant:42");
                    assertThat(config.matcherConfig().symbolId()).isEqualTo(7);
                    assertThat(config.serverMode()).isEqualTo(MatcherServerMode.PROD);
                    assertThat(config.httpPort()).isEqualTo(18080);
                    assertThat(config.httpBindHost()).isEqualTo("127.0.0.1");
                    assertThat(config.grpcPort()).isEqualTo(19090);
                    assertThat(config.grpcServerConfig().bindHost()).isEqualTo("127.0.0.1");
                    assertThat(config.httpWorkerThreads()).isEqualTo(6);
                    assertThat(config.httpMaxBodyBytes()).isEqualTo(4096);
                    assertThat(config.httpMaxConcurrentRequests()).isEqualTo(128);
                    assertThat(config.httpRequestTimeoutMillis()).isEqualTo(1500L);
                    assertThat(config.httpWriteMaxConcurrentRequests()).isEqualTo(64);
                    assertThat(config.httpReadMaxConcurrentRequests()).isEqualTo(48);
                    assertThat(config.httpAdminMaxConcurrentRequests()).isEqualTo(8);
                    assertThat(config.httpWriteTimeoutMillis()).isEqualTo(2100L);
                    assertThat(config.httpReadTimeoutMillis()).isEqualTo(1200L);
                    assertThat(config.httpAdminTimeoutMillis()).isEqualTo(5500L);
                    assertThat(config.httpSubmitEndpointMaxConcurrentRequests()).isEqualTo(40);
                    assertThat(config.httpCancelEndpointMaxConcurrentRequests()).isEqualTo(24);
                    assertThat(config.httpSnapshotEndpointMaxConcurrentRequests()).isEqualTo(3);
                    assertThat(config.httpReadinessEndpointMaxConcurrentRequests()).isEqualTo(12);
                    assertThat(config.httpMetricsEndpointMaxConcurrentRequests()).isEqualTo(6);
                    assertThat(config.httpShardWriteMaxConcurrentRequests()).isEqualTo(50);
                    assertThat(config.httpTenantWriteMaxConcurrentRequests()).isEqualTo(7);
                    assertThat(config.httpTenantAdmissionHeader()).isEqualTo("X-Tenant-Key");
                    assertThat(config.writeAdmissionPolicyConfig().shardRateLimitPerSecond()).isEqualTo(120.5d);
                    assertThat(config.writeAdmissionPolicyConfig().shardRateBurst()).isEqualTo(24);
                    assertThat(config.writeAdmissionPolicyConfig().tenantRateLimitPerSecond()).isEqualTo(40.0d);
                    assertThat(config.writeAdmissionPolicyConfig().tenantRateBurst()).isEqualTo(8);
                    assertThat(config.writeAdmissionPolicyConfig().tenantDefaultWeight()).isEqualTo(2);
                    assertThat(config.writeAdmissionPolicyConfig().tenantWeightOverrides()).isEqualTo("vip-a=5,vip-b=3");
                    assertThat(config.writeAdmissionPolicyConfig().tenantPriorityHeader()).isEqualTo("X-Tenant-Priority");
                    assertThat(config.walDurabilityMode()).isEqualTo(WalDurabilityMode.SYNC_PER_BATCH);
                    assertThat(config.walForceBatchSize()).isEqualTo(32);

                    assertThat(securityConfig.grpcServerTls()).isNotNull();
                    assertThat(securityConfig.grpcServerTls().certificateChainFile()).hasToString("/tmp/server.crt");
                    assertThat(securityConfig.grpcServerTls().privateKeyFile()).hasToString("/tmp/server.key");
                    assertThat(securityConfig.grpcServerTls().trustCertCollectionFile()).hasToString("/tmp/ca.crt");
                    assertThat(securityConfig.grpcServerTls().requireMutualTls()).isTrue();
                    assertThat(securityConfig.tlsReloadIntervalMillis()).isEqualTo(5000L);
                    assertThat(securityConfig.openTelemetryMetricsEnabled()).isTrue();

                    assertThat(ttlConfig.enabled()).isTrue();
                    assertThat(ttlConfig.sweepIntervalMillis()).isEqualTo(250L);
                    assertThat(ttlConfig.defaultTtlMillis()).isEqualTo(1000L);
                    assertThat(ttlConfig.hardTtlMillis()).isEqualTo(2000L);
                    assertThat(ttlConfig.recoveredOrderTtlMillis()).isEqualTo(3000L);
                    assertThat(ttlConfig.recentAuditLimit()).isEqualTo(16);
                });
    }

    @Test
    void bindsClusterPropertiesWhenLeaseStoreAndRegistryProvided() {
        contextRunner
                .withBean(LeaseStore.class, InMemoryLeaseStore::new)
                .withBean(NodeRegistry.class, InMemoryNodeRegistry::new)
                .withPropertyValues(
                        "ull.matcher.cluster.enabled=true",
                        "ull.matcher.cluster.advertised-host=10.0.0.12",
                        "ull.matcher.cluster.coordinator-tick-millis=125",
                        "ull.matcher.cluster.discovery-rpc-timeout-millis=900",
                        "ull.matcher.cluster.lease-ttl-millis=3000",
                        "ull.matcher.cluster.failover-primary-heartbeat-timeout-millis=2500",
                        "ull.matcher.cluster.failover-max-promotion-lag=7",
                        "ull.matcher.cluster.failover-min-standby-replicas=2",
                        "ull.matcher.cluster.snapshot-sync-threshold=42",
                        "ull.matcher.cluster.snapshot-sync-timeout-millis=6000",
                        "ull.matcher.cluster.replication-mode=WAIT_FOR_ALL_STANDBYS",
                        "ull.matcher.cluster.replication-timeout-millis=700",
                        "ull.matcher.cluster.replication-transport=AERON_PREVIEW",
                        "ull.matcher.cluster.aeron-preview.directory=target/test-aeron-preview",
                        "ull.matcher.cluster.aeron-preview.port=15090",
                        "ull.matcher.cluster.aeron-preview.stream-id=12001",
                        "ull.matcher.cluster.transport-policy.allow-transport-change=true",
                        "ull.matcher.cluster.transport-policy.transport-change-window-id=change-20260621",
                        "ull.matcher.cluster.transport-policy.allow-preview-transport-in-prod=true"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(MatcherClusterConfig.class);
                    MatcherClusterConfig clusterConfig = context.getBean(MatcherClusterConfig.class);
                    MatcherServerConfig serverConfig = context.getBean(MatcherServerConfig.class);

                    assertThat(clusterConfig.shardKey()).isEqualTo("symbol-1");
                    assertThat(clusterConfig.advertisedHost()).isEqualTo("10.0.0.12");
                    assertThat(clusterConfig.coordinatorTickMillis()).isEqualTo(125L);
                    assertThat(clusterConfig.discoveryRpcTimeoutNanos()).isEqualTo(900_000_000L);
                    assertThat(clusterConfig.leaseTtlNanos()).isEqualTo(3_000_000_000L);
                    assertThat(clusterConfig.failoverPolicy().primaryHeartbeatTimeoutNanos()).isEqualTo(2_500_000_000L);
                    assertThat(clusterConfig.failoverPolicy().maxPromotionLag()).isEqualTo(7L);
                    assertThat(clusterConfig.failoverPolicy().minStandbyReplicas()).isEqualTo(2);
                    assertThat(clusterConfig.snapshotSyncThreshold()).isEqualTo(42L);
                    assertThat(clusterConfig.snapshotSyncTimeoutNanos()).isEqualTo(6_000_000_000L);
                    assertThat(clusterConfig.replicationMode()).isEqualTo(ReplicationMode.WAIT_FOR_ALL_STANDBYS);
                    assertThat(clusterConfig.replicationTimeoutNanos()).isEqualTo(700_000_000L);
                    assertThat(clusterConfig.replicationTransportType()).isEqualTo(ReplicationTransportType.AERON_PREVIEW);
                    assertThat(clusterConfig.aeronPreviewTransportConfig().directory()).hasToString("target/test-aeron-preview");
                    assertThat(clusterConfig.aeronPreviewTransportConfig().port()).isEqualTo(15090);
                    assertThat(clusterConfig.aeronPreviewTransportConfig().streamId()).isEqualTo(12001);
                    assertThat(clusterConfig.replicationTransportPolicyConfig().allowTransportChange()).isTrue();
                    assertThat(clusterConfig.replicationTransportPolicyConfig().transportChangeWindowId()).isEqualTo("change-20260621");
                    assertThat(clusterConfig.replicationTransportPolicyConfig().allowPreviewTransportInProd()).isTrue();
                    assertThat(serverConfig.clusterConfig()).isSameAs(clusterConfig);
                });
    }

    private static final class InMemoryLeaseStore implements LeaseStore {
        private ClusterLease currentLease;

        @Override
        public ClusterLease currentLease() {
            return currentLease;
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            currentLease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
            return true;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            currentLease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
            return true;
        }
    }

    private static final class InMemoryNodeRegistry implements NodeRegistry {
        @Override
        public void registerOrUpdate(DiscoveredNode node) throws IOException {}

        @Override
        public void unregister(String nodeId) throws IOException {}

        @Override
        public List<DiscoveredNode> listNodes() {
            return List.of();
        }
    }
}
