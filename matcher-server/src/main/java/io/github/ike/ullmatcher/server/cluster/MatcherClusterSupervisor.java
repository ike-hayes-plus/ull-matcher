package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.HaTickResult;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.readiness.StandbyReadinessGate;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.state.ReplicaState;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public final class MatcherClusterSupervisor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(MatcherClusterSupervisor.class);
    private static final int MAX_RECENT_ERRORS = 8;
    private static final long MIN_REMOTE_PROBE_INTERVAL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);
    private static final long MIN_PRIMARY_REMOTE_PROBE_INTERVAL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(5);

    private final MatcherServerConfig config;
    private final MatcherNodeService nodeService;
    private final MatcherClusterConfig clusterConfig;
    private final ReplicationTransportProvider transportProvider;
    private final DiscoveryDrivenReplicator replicator;
    private final Map<String, ControlClientRef> controlClients = new LinkedHashMap<>();
    private final Map<String, ReplicaProbeCacheEntry> replicaProbeCache = new LinkedHashMap<>();
    private final StandbyReadinessGate readinessGate = new StandbyReadinessGate();
    private final Thread thread;
    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicLong tickFailureCount = new AtomicLong();
    private final Map<String, AtomicLong> errorCounts = new java.util.concurrent.ConcurrentHashMap<>();
    private final Deque<String> recentErrors = new ConcurrentLinkedDeque<>();

    private volatile boolean running = true;
    private volatile HaTickResult lastTickResult;
    private volatile StandbyReadinessGate.GateDecision lastGateDecision;
    private volatile String syncState = "IDLE";
    private volatile String lastSyncError = "";
    private volatile String transportPolicyStatus = "STABLE";
    private volatile String transportPolicyConclusion = "transport policy is stable";
    private final long remoteProbeIntervalNanos;
    private final long primaryRemoteProbeIntervalNanos;

    public MatcherClusterSupervisor(MatcherServerConfig config, MatcherNodeService nodeService,
                                    ReplicationTransportProvider transportProvider) throws IOException {
        this.config = config;
        this.nodeService = nodeService;
        this.clusterConfig = config.clusterConfig();
        this.transportProvider = Objects.requireNonNull(transportProvider, "transportProvider");
        this.replicator = new DiscoveryDrivenReplicator(config.nodeId(), transportProvider);
        this.remoteProbeIntervalNanos = Math.max(
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(clusterConfig.coordinatorTickMillis() * 4L),
                MIN_REMOTE_PROBE_INTERVAL_NANOS
        );
        this.primaryRemoteProbeIntervalNanos = Math.max(
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(clusterConfig.coordinatorTickMillis() * 20L),
                MIN_PRIMARY_REMOTE_PROBE_INTERVAL_NANOS
        );
        this.nodeService.configureReplication(replicator, clusterConfig.replicationMode(), clusterConfig.replicationTimeoutNanos());
        this.thread = Thread.ofPlatform().name("cluster-supervisor-" + config.nodeId()).start(this::runLoop);
    }

    private void runLoop() {
        while (running) {
            try {
                tickCount.incrementAndGet();
                tick();
            } catch (IOException | RuntimeException ignored) {
                if (!running && isExpectedShutdownFailure(ignored)) {
                    return;
                }
                tickFailureCount.incrementAndGet();
                recordError("tick_loop", ignored);
            }
            try {
                Thread.sleep(clusterConfig.coordinatorTickMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void tick() throws IOException {
        NodeControlState localState = nodeService.currentState();
        try {
            Map<String, String> metadata = new LinkedHashMap<>(transportProvider.localNodeMetadata());
            if (clusterConfig.replicationTransportPolicyConfig().changeWindowActive()) {
                metadata.put(
                        ReplicationTransportProvider.TRANSPORT_CHANGE_WINDOW_METADATA_KEY,
                        clusterConfig.replicationTransportPolicyConfig().transportChangeWindowId()
                );
            }
            metadata.put("updatedAt", Instant.now().toString());
            metadata.put("shardKey", clusterConfig.shardKey());
            metadata.put("symbolId", Integer.toString(config.matcherConfig().symbolId()));
            clusterConfig.nodeRegistry().registerOrUpdate(new DiscoveredNode(
                    localState.nodeId(),
                    clusterConfig.advertisedHost(),
                    config.grpcPort(),
                    localState.role(),
                    Map.copyOf(metadata)
            ));
        } catch (IOException e) {
            recordError("registry_update", e);
            throw e;
        }

        List<DiscoveredNode> discoveredNodes;
        try {
            discoveredNodes = clusterConfig.nodeRegistry().listNodes().stream()
                    .filter(this::isSameShard)
                    .sorted(Comparator.comparing(DiscoveredNode::nodeId))
                    .toList();
        } catch (IOException e) {
            recordError("discovery_list", e);
            throw e;
        }
        assessTransportPolicy(discoveredNodes);
        replicator.refresh(discoveredNodes);

        List<ReplicaState> standbyStates = new ArrayList<>();
        ReplicaState observedPrimary = null;
        ClusterLease lease = clusterConfig.leaseStore().currentLease();
        long now = System.nanoTime();

        boolean localPrimaryFastPath = localState.role() == HaRole.PRIMARY
                && lease != null
                && config.nodeId().equals(lease.ownerNodeId());
        for (DiscoveredNode discovered : discoveredNodes) {
            ReplicaState replica = toReplicaState(discovered, now, localPrimaryFastPath);
            if (replica.role() == HaRole.PRIMARY) {
                if (lease != null && lease.ownerNodeId().equals(replica.nodeId())) {
                    observedPrimary = replica;
                }
            } else if (!discovered.nodeId().equals(config.nodeId()) || localState.role() != HaRole.PRIMARY) {
                standbyStates.add(replica);
            }
        }

        boolean localPromotionReady = true;
        if (localState.role() != HaRole.PRIMARY && observedPrimary != null) {
            localPromotionReady = ensureLocalCatchUp(observedPrimary);
        }
        localState = nodeService.currentState();
        if (localState.role() != HaRole.PRIMARY) {
            if (localPromotionReady) {
                standbyStates.add(localReplicaState(localState, now));
            }
        } else if (observedPrimary == null) {
            observedPrimary = localReplicaState(localState, now);
        }

        if (observedPrimary == null) {
            FencingToken token = lease != null ? lease.fencingToken() : new FencingToken(1L);
            observedPrimary = new ReplicaState("unknown-primary", HaRole.PRIMARY, false, false, 0L, token, localState.cursor());
        }

        HaTickResult result = nodeService.tickHa(
                clusterConfig.leaseStore(),
                clusterConfig.failoverPolicy(),
                clusterConfig.leaseTtlNanos(),
                observedPrimary,
                standbyStates,
                now
        );
        lastTickResult = result;
        if (result.roleAfter() == HaRole.STANDBY || result.roleAfter() == HaRole.CATCHING_UP) {
            nodeService.clearReplication();
        } else if (result.roleAfter() == HaRole.PRIMARY) {
            nodeService.configureReplication(replicator, clusterConfig.replicationMode(), clusterConfig.replicationTimeoutNanos());
        }
    }

    private ReplicaState toReplicaState(DiscoveredNode discoveredNode, long nowNanos, boolean localPrimaryFastPath) throws IOException {
        if (discoveredNode.nodeId().equals(config.nodeId())) {
            return localReplicaState(nodeService.currentState(), nowNanos);
        }
        ReplicaProbeCacheEntry cached = replicaProbeCache.get(discoveredNode.nodeId());
        long probeIntervalNanos = localPrimaryFastPath ? primaryRemoteProbeIntervalNanos : remoteProbeIntervalNanos;
        if (cached != null && nowNanos - cached.observedAtNanos() < probeIntervalNanos) {
            return cached.replicaState();
        }
        if (localPrimaryFastPath && discoveredNode.role() != HaRole.PRIMARY) {
            ReplicaState replicaState = discoveryReplicaState(discoveredNode, nowNanos);
            replicaProbeCache.put(discoveredNode.nodeId(), new ReplicaProbeCacheEntry(replicaState, nowNanos));
            return replicaState;
        }
        ClusterPeerClient client = controlClientFor(discoveredNode);
        try {
            NodeControlState state = client.fetchNodeState(clusterConfig.discoveryRpcTimeoutNanos());
            ReplicaState replicaState = new ReplicaState(
                    state.nodeId(),
                    state.role(),
                    true,
                    state.loopState() != io.github.ike.ullmatcher.runtime.MatchLoopState.FAILED,
                    nowNanos,
                    state.fencingToken(),
                    state.cursor()
            );
            replicaProbeCache.put(discoveredNode.nodeId(), new ReplicaProbeCacheEntry(replicaState, nowNanos));
            return replicaState;
        } catch (IOException e) {
            recordError("replica_probe", e);
            ReplicaState replicaState = new ReplicaState(
                    discoveredNode.nodeId(),
                    discoveredNode.role(),
                    false,
                    false,
                    0L,
                    new FencingToken(1L),
                    nodeService.currentState().cursor()
            );
            replicaProbeCache.put(discoveredNode.nodeId(), new ReplicaProbeCacheEntry(replicaState, nowNanos));
            return replicaState;
        }
    }

    private static ReplicaState discoveryReplicaState(DiscoveredNode discoveredNode, long nowNanos) {
        return new ReplicaState(
                discoveredNode.nodeId(),
                discoveredNode.role(),
                true,
                true,
                nowNanos,
                new FencingToken(1L),
                new io.github.ike.ullmatcher.ha.replication.ReplicationCursor(0L, 0L, 0L, 0L)
        );
    }

    private boolean ensureLocalCatchUp(ReplicaState observedPrimary) throws IOException {
        NodeControlState localState = nodeService.currentState();
        StandbyReadinessGate.GateDecision decision = readinessGate.evaluate(
                observedPrimary.cursor(),
                localState.cursor(),
                clusterConfig.readinessPolicy(),
                clusterConfig.snapshotSyncThreshold()
        );
        lastGateDecision = decision;
        if (decision.promotionReady()) {
            syncState = "READY";
            return true;
        }
        nodeService.beginCatchUp();
        syncState = "CATCHING_UP";
        if (decision.snapshotSyncRequired()) {
            syncState = "SNAPSHOT_SYNC";
            DiscoveredNode primaryNode = clusterConfig.nodeRegistry().listNodes().stream()
                    .filter(this::isSameShard)
                    .filter(node -> node.nodeId().equals(observedPrimary.nodeId()))
                    .findFirst()
                    .orElse(null);
            if (primaryNode != null) {
                ClusterPeerClient target = controlClientFor(primaryNode);
                try {
                    nodeService.installSnapshotFrom(target, clusterConfig.snapshotSyncTimeoutNanos());
                    lastSyncError = "";
                } catch (IOException e) {
                    lastSyncError = e.getMessage();
                    recordError("snapshot_sync", e);
                    throw e;
                }
            }
        }
        boolean ready = readinessGate.evaluate(
                observedPrimary.cursor(),
                nodeService.currentState().cursor(),
                clusterConfig.readinessPolicy(),
                clusterConfig.snapshotSyncThreshold()
        ).promotionReady();
        syncState = ready ? "READY" : "CATCHING_UP";
        return ready;
    }

    public ClusterSupervisorMetricsSnapshot metricsSnapshot() {
        Map<String, Long> counts = new LinkedHashMap<>();
        errorCounts.forEach((key, value) -> counts.put(key, value.get()));
        return new ClusterSupervisorMetricsSnapshot(
                tickCount.get(),
                tickFailureCount.get(),
                lastTickResult,
                lastGateDecision,
                Collections.unmodifiableMap(counts),
                List.copyOf(recentErrors),
                syncState,
                lastSyncError,
                transportProvider.metricsSnapshot().withPolicy(transportPolicyStatus, transportPolicyConclusion)
        );
    }

    private void recordError(String category, Throwable error) {
        long count = errorCounts.computeIfAbsent(category, ignored -> new AtomicLong()).incrementAndGet();
        recentErrors.addFirst(category + ": " + error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage()));
        while (recentErrors.size() > MAX_RECENT_ERRORS) {
            recentErrors.removeLast();
        }
        if (count == 1L || count % 100L == 0L) {
            LOG.warn("cluster supervisor error category={} count={} message={}",
                    category, count, String.valueOf(error.getMessage()));
        }
    }

    private boolean isSameShard(DiscoveredNode node) {
        return clusterConfig.shardKey().equals(node.metadata().get("shardKey"));
    }

    private void assessTransportPolicy(List<DiscoveredNode> discoveredNodes) {
        ReplicationTransportPolicyConfig policy = clusterConfig.replicationTransportPolicyConfig();
        String localTransport = clusterConfig.replicationTransportType().name();
        List<String> incompatiblePeers = new ArrayList<>();
        List<String> transitionalPeers = new ArrayList<>();
        for (DiscoveredNode node : discoveredNodes) {
            if (node.nodeId().equals(config.nodeId())) {
                continue;
            }
            String peerTransport = node.metadata().getOrDefault(ReplicationTransportProvider.TRANSPORT_METADATA_KEY, "");
            if (peerTransport.equals(localTransport)) {
                continue;
            }
            String peerWindowId = node.metadata().getOrDefault(ReplicationTransportProvider.TRANSPORT_CHANGE_WINDOW_METADATA_KEY, "");
            String description = node.nodeId() + "[" + (peerTransport.isBlank() ? "UNSET" : peerTransport) + "]";
            if (policy.changeWindowActive() && policy.transportChangeWindowId().equals(peerWindowId)) {
                transitionalPeers.add(description);
            } else {
                incompatiblePeers.add(description);
            }
        }
        if (!incompatiblePeers.isEmpty()) {
            transportPolicyStatus = "DRIFT";
            transportPolicyConclusion = "replication transport drift detected for shard "
                    + clusterConfig.shardKey() + ": " + String.join(", ", incompatiblePeers);
            syncState = "TRANSPORT_DRIFT";
            lastSyncError = transportPolicyConclusion;
            if (config.serverMode() == io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode.PROD) {
                throw new IllegalStateException(transportPolicyConclusion);
            }
            return;
        }
        if (!transitionalPeers.isEmpty()) {
            transportPolicyStatus = "CHANGE_WINDOW";
            transportPolicyConclusion = "replication transport change window active: " + String.join(", ", transitionalPeers);
            return;
        }
        transportPolicyStatus = "STABLE";
        transportPolicyConclusion = "transport policy is stable";
    }

    private static boolean isExpectedShutdownFailure(Throwable error) {
        return error instanceof IOException && String.valueOf(error.getMessage()).contains("interrupted while waiting for matcher control task");
    }

    private static ReplicaState localReplicaState(NodeControlState state, long nowNanos) {
        return new ReplicaState(
                state.nodeId(),
                state.role(),
                true,
                state.loopState() != io.github.ike.ullmatcher.runtime.MatchLoopState.FAILED,
                nowNanos,
                state.fencingToken(),
                state.cursor()
        );
    }

    @Override
    public void close() throws IOException {
        running = false;
        thread.interrupt();
        try {
            thread.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while stopping cluster supervisor", e);
        }
        IOException failure = null;
        try {
            clusterConfig.nodeRegistry().unregister(config.nodeId());
        } catch (IOException e) {
            failure = e;
        }
        try {
            replicator.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        for (ControlClientRef target : controlClients.values()) {
            try {
                target.target().close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        clusterConfig.nodeRegistry().close();
        if (clusterConfig.leaseStore() instanceof Closeable closeable) {
            closeable.close();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private synchronized ClusterPeerClient controlClientFor(DiscoveredNode discoveredNode) throws IOException {
        ControlClientRef existing = controlClients.get(discoveredNode.nodeId());
        if (existing != null && existing.matches(discoveredNode)) {
            return existing.target();
        }
        if (existing != null) {
            existing.target().close();
        }
        LOG.info("refreshing control client {} -> {}:{}", discoveredNode.nodeId(), discoveredNode.host(), discoveredNode.grpcPort());
        ClusterPeerClient target = transportProvider.connect(discoveredNode);
        controlClients.put(discoveredNode.nodeId(), new ControlClientRef(discoveredNode.host(), discoveredNode.grpcPort(), target));
        return target;
    }

    private record ControlClientRef(String host, int port, ClusterPeerClient target) {
        private boolean matches(DiscoveredNode node) {
            return port == node.grpcPort() && host.equals(node.host());
        }
    }

    private record ReplicaProbeCacheEntry(ReplicaState replicaState, long observedAtNanos) {
    }
}
