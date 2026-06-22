package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.readiness.PromotionReadinessPolicy;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 描述集群模式下主备复制、节点发现与传输策略所需的服务端配置。
 */
public record MatcherClusterConfig(
        LeaseStore leaseStore,
        NodeRegistry nodeRegistry,
        String shardKey,
        String advertisedHost,
        long coordinatorTickMillis,
        long discoveryRpcTimeoutNanos,
        long leaseTtlNanos,
        FailoverPolicy failoverPolicy,
        PromotionReadinessPolicy readinessPolicy,
        long snapshotSyncThreshold,
        long snapshotSyncTimeoutNanos,
        ReplicationMode replicationMode,
        long replicationTimeoutNanos,
        ReplicationTransportType replicationTransportType,
        AeronPreviewTransportConfig aeronPreviewTransportConfig,
        ReplicationTransportPolicyConfig replicationTransportPolicyConfig
) {
    public MatcherClusterConfig {
        Objects.requireNonNull(leaseStore, "leaseStore");
        Objects.requireNonNull(nodeRegistry, "nodeRegistry");
        Objects.requireNonNull(shardKey, "shardKey");
        Objects.requireNonNull(advertisedHost, "advertisedHost");
        Objects.requireNonNull(failoverPolicy, "failoverPolicy");
        Objects.requireNonNull(readinessPolicy, "readinessPolicy");
        Objects.requireNonNull(replicationMode, "replicationMode");
        Objects.requireNonNull(replicationTransportType, "replicationTransportType");
        Objects.requireNonNull(aeronPreviewTransportConfig, "aeronPreviewTransportConfig");
        Objects.requireNonNull(replicationTransportPolicyConfig, "replicationTransportPolicyConfig");
        if (advertisedHost.isBlank() || shardKey.isBlank()) {
            throw new IllegalArgumentException("shardKey and advertisedHost must not be blank");
        }
        if (coordinatorTickMillis <= 0L || discoveryRpcTimeoutNanos <= 0L || leaseTtlNanos <= 0L
                || replicationTimeoutNanos < 0L || snapshotSyncThreshold < 0L || snapshotSyncTimeoutNanos <= 0L) {
            throw new IllegalArgumentException("cluster timing values must be positive");
        }
    }

    public static MatcherClusterConfig defaults(LeaseStore leaseStore, NodeRegistry nodeRegistry, String advertisedHost, String shardKey) {
        return new MatcherClusterConfig(
                leaseStore,
                nodeRegistry,
                shardKey,
                advertisedHost,
                250L,
                TimeUnit.SECONDS.toNanos(1),
                TimeUnit.SECONDS.toNanos(5),
                FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(),
                0L,
                TimeUnit.SECONDS.toNanos(5),
                ReplicationMode.WAIT_FOR_ANY_STANDBY,
                TimeUnit.SECONDS.toNanos(5),
                ReplicationTransportType.GRPC,
                new AeronPreviewTransportConfig(Path.of("target", "matcher-aeron-preview"), 15_090, 11_001),
                ReplicationTransportPolicyConfig.defaults()
        );
    }

    public MatcherClusterConfig withReplicationTransport(ReplicationTransportType transportType,
                                                         AeronPreviewTransportConfig aeronConfig,
                                                         ReplicationTransportPolicyConfig policyConfig) {
        return new MatcherClusterConfig(
                leaseStore,
                nodeRegistry,
                shardKey,
                advertisedHost,
                coordinatorTickMillis,
                discoveryRpcTimeoutNanos,
                leaseTtlNanos,
                failoverPolicy,
                readinessPolicy,
                snapshotSyncThreshold,
                snapshotSyncTimeoutNanos,
                replicationMode,
                replicationTimeoutNanos,
                transportType,
                aeronConfig,
                policyConfig
        );
    }
}
