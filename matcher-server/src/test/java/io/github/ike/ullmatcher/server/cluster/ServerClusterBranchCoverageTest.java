/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.readiness.PromotionReadinessPolicy;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ServerClusterBranchCoverageTest {
    @Test
    void clusterConfigRejectsNullBlankAndNonPositiveTiming() {
        LeaseStore leaseStore = new StubLeaseStore();
        NodeRegistry registry = new StubNodeRegistry();
        MatcherClusterConfig valid = MatcherClusterConfig.defaults(leaseStore, registry, "127.0.0.1", "symbol-1");
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                null, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                leaseStore, null, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, null,
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                null, 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, null, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                null, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, null,
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(NullPointerException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                null));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, " ", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", " ", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 0, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 0, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 0, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, -1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), -1, 1, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClusterConfig(
                leaseStore, registry, "s", "h", 1, 1, 1, FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(), 0, 0, ReplicationMode.WAIT_FOR_ANY_STANDBY, 1,
                ReplicationTransportType.GRPC, valid.aeronPreviewTransportConfig(),
                ReplicationTransportPolicyConfig.defaults()));

        MatcherClusterConfig aeron = valid.withReplicationTransport(
                ReplicationTransportType.AERON,
                valid.aeronPreviewTransportConfig(),
                new ReplicationTransportPolicyConfig(true, "w1", false));
        assertEquals(ReplicationTransportType.AERON, aeron.replicationTransportType());
        assertTrue(aeron.replicationTransportPolicyConfig().changeWindowActive());
        assertFalse(ReplicationTransportPolicyConfig.defaults().changeWindowActive());
        assertFalse(new ReplicationTransportPolicyConfig(true, " ", false).changeWindowActive());
    }

    @Test
    void transportPolicyEnforcerLocksAndRejectsUnsafeChanges(@TempDir Path dir) throws Exception {
        MatcherServerConfig standalone = MatcherServerConfig.defaults("node-a", 1, dir);
        ReplicationTransportPolicyEnforcer.validateAndLock(standalone);

        MatcherClusterConfig grpc = MatcherClusterConfig.defaults(
                new StubLeaseStore(), new StubNodeRegistry(), "127.0.0.1", standalone.shardKey());
        MatcherServerConfig clustered = standalone.withClusterConfig(grpc);
        ReplicationTransportPolicyEnforcer.validateAndLock(clustered);
        ReplicationTransportPolicyEnforcer.validateAndLock(clustered);

        MatcherServerConfig previewForbidden = MatcherServerConfig.builder("node-a", 1, dir)
                .serverMode(MatcherServerMode.PROD)
                .clusterConfig(grpc.withReplicationTransport(
                        ReplicationTransportType.AERON_PREVIEW,
                        grpc.aeronPreviewTransportConfig(),
                        ReplicationTransportPolicyConfig.defaults()))
                .build();
        assertThrows(IllegalStateException.class,
                () -> ReplicationTransportPolicyEnforcer.validateAndLock(previewForbidden));

        Path lockFile = clustered.walDirectory().resolveSibling("replication-transport.lock");
        Files.writeString(lockFile, "transport=\nwindowId=\n");
        ReplicationTransportPolicyEnforcer.validateAndLock(clustered);

        Files.writeString(lockFile, "transport=AERON\nwindowId=old\n");
        assertThrows(IllegalStateException.class,
                () -> ReplicationTransportPolicyEnforcer.validateAndLock(clustered));

        MatcherServerConfig allowedChange = clustered.withClusterConfig(grpc.withReplicationTransport(
                ReplicationTransportType.GRPC,
                grpc.aeronPreviewTransportConfig(),
                new ReplicationTransportPolicyConfig(true, "window-2", false)));
        Files.writeString(lockFile, "transport=AERON\nwindowId=old\n");
        ReplicationTransportPolicyEnforcer.validateAndLock(allowedChange);
        assertTrue(Files.readString(lockFile).contains("GRPC"));
    }

    @Test
    void previewReconciliationCoversIdleLagAheadAndOutOfOrder() {
        AtomicLong authoritative = new AtomicLong();
        AeronPreviewReconciliationTracker tracker = new AeronPreviewReconciliationTracker(authoritative::get);
        assertEquals("IDLE", tracker.enrich(TransportMetricsSnapshot.none("AERON_PREVIEW")).reconciliationStatus());

        authoritative.set(4L);
        tracker.recordPreviewSequence(2L);
        assertEquals("PREVIEW_LAGGING", tracker.enrich(TransportMetricsSnapshot.none("AERON_PREVIEW")).reconciliationStatus());

        tracker.recordPreviewSequence(1L);
        assertEquals("DIVERGED", tracker.enrich(TransportMetricsSnapshot.none("AERON_PREVIEW")).reconciliationStatus());
        assertEquals(1L, tracker.enrich(TransportMetricsSnapshot.none("AERON_PREVIEW")).previewOutOfOrderCount());

        AeronPreviewReconciliationTracker ahead = new AeronPreviewReconciliationTracker(() -> 1L);
        ahead.recordPreviewSequence(3L);
        assertEquals("PREVIEW_AHEAD", ahead.enrich(TransportMetricsSnapshot.none("AERON_PREVIEW")).reconciliationStatus());
    }

    private static final class StubLeaseStore implements LeaseStore {
        @Override
        public ClusterLease currentLease() {
            return new ClusterLease("node-a", new FencingToken(1L), System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            return true;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            return true;
        }
    }

    private static final class StubNodeRegistry implements NodeRegistry {
        @Override
        public void registerOrUpdate(DiscoveredNode node) {
        }

        @Override
        public void unregister(String nodeId) {
        }

        @Override
        public List<DiscoveredNode> listNodes() {
            return List.of();
        }
    }
}
