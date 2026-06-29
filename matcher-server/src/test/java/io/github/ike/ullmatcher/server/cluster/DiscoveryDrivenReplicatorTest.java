package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServer;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DiscoveryDrivenReplicatorTest {
    @Test
    @Tag("chaos")
    void refreshRebuildsTargetWhenEndpointChanges() throws Exception {
        try (StandbyFixture first = new StandbyFixture("standby-a");
             StandbyFixture second = new StandbyFixture("standby-a");
             DiscoveryDrivenReplicator replicator = new DiscoveryDrivenReplicator(
                     "primary-a",
                     new GrpcReplicationTransportProvider(io.github.ike.ullmatcher.server.security.ServerSecurityConfig.insecureDefaults()))) {
            replicator.refresh(List.of(new DiscoveredNode("standby-a", "127.0.0.1", first.port(), HaRole.STANDBY, java.util.Map.of())));
            replicator.replicate(command(1L), TimeUnit.SECONDS.toNanos(1));
            assertEquals(1, first.wal.appendCount);
            assertEquals(0, second.wal.appendCount);

            replicator.refresh(List.of(new DiscoveredNode("standby-a", "127.0.0.1", second.port(), HaRole.STANDBY, java.util.Map.of())));
            replicator.replicate(command(2L), TimeUnit.SECONDS.toNanos(1));

            assertEquals(1, first.wal.appendCount);
            assertEquals(1, second.wal.appendCount);
        }
    }

    @Test
    void replicateBatchAsyncCompletesOnceQuorumIsReached() throws Exception {
        ControlledAsyncTarget standbyA = new ControlledAsyncTarget("standby-a");
        ControlledAsyncTarget standbyB = new ControlledAsyncTarget("standby-b");
        ControlledAsyncTarget standbyC = new ControlledAsyncTarget("standby-c");
        try (DiscoveryDrivenReplicator replicator = new DiscoveryDrivenReplicator("primary-a", new ControlledTransportProvider(Map.of(
                "standby-a", standbyA,
                "standby-b", standbyB,
                "standby-c", standbyC
        )))) {
            replicator.refresh(List.of(
                    new DiscoveredNode("standby-a", "127.0.0.1", 9001, HaRole.STANDBY, Map.of()),
                    new DiscoveredNode("standby-b", "127.0.0.1", 9002, HaRole.STANDBY, Map.of()),
                    new DiscoveredNode("standby-c", "127.0.0.1", 9003, HaRole.STANDBY, Map.of())
            ));
            CompletableFuture<ReplicationResult> future = replicator.replicateBatchAsync(
                    List.of(command(1L), command(2L)),
                    ReplicationMode.WAIT_FOR_QUORUM_STANDBYS,
                    TimeUnit.SECONDS.toNanos(1)
            );
            standbyA.succeed();
            assertFalse(future.isDone());
            standbyB.succeed();
            ReplicationResult result = future.get(1, TimeUnit.SECONDS);
            assertEquals(2, result.ackedTargets());
            assertTrue(result.satisfies(ReplicationMode.WAIT_FOR_QUORUM_STANDBYS));
            assertFalse(result.ackedNodeIds().contains("standby-c"));
            standbyC.succeed();
        }
    }

    @Test
    void preferredBatchHintsStayLargeForGrpc() throws Exception {
        try (DiscoveryDrivenReplicator replicator = new DiscoveryDrivenReplicator("primary-a", new ControlledTransportProvider(
                ReplicationTransportType.GRPC,
                Map.of("standby-a", new ControlledAsyncTarget("standby-a"), "standby-b", new ControlledAsyncTarget("standby-b"))))) {
            replicator.refresh(List.of(
                    new DiscoveredNode("standby-a", "127.0.0.1", 9001, HaRole.STANDBY, Map.of()),
                    new DiscoveredNode("standby-b", "127.0.0.1", 9002, HaRole.STANDBY, Map.of())
            ));
            assertEquals(2_048, replicator.preferredMaxBatchSize());
            assertEquals(16, replicator.preferredInFlightBatches());
        }
    }

    @Test
    void preferredBatchHintsUseBoundedBatchAndFullWindowForAeronMultiStandby() throws Exception {
        try (DiscoveryDrivenReplicator replicator = new DiscoveryDrivenReplicator("primary-a", new ControlledTransportProvider(
                ReplicationTransportType.AERON,
                Map.of(
                        "standby-a", new ControlledAsyncTarget("standby-a"),
                        "standby-b", new ControlledAsyncTarget("standby-b"),
                        "standby-c", new ControlledAsyncTarget("standby-c"))))) {
            replicator.refresh(List.of(
                    new DiscoveredNode("standby-a", "127.0.0.1", 9001, HaRole.STANDBY, Map.of()),
                    new DiscoveredNode("standby-b", "127.0.0.1", 9002, HaRole.STANDBY, Map.of()),
                    new DiscoveredNode("standby-c", "127.0.0.1", 9003, HaRole.STANDBY, Map.of())
            ));
            assertEquals(256, replicator.preferredMaxBatchSize());
            assertEquals(16, replicator.preferredInFlightBatches());
            assertEquals(0L, replicator.preferredAccumulationNanos());
        }
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 10_000L + sequence, 1L, 1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
    }

    private static final class StandbyFixture implements AutoCloseable {
        private final RecordingWalWriter wal = new RecordingWalWriter();
        private final SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        private final UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        private final MatchLoop loop = new MatchLoop(ring, matcher);
        private final Thread loopThread = Thread.ofPlatform().start(loop);
        private final io.github.ike.ullmatcher.ha.standby.StandbySyncService standby =
                new io.github.ike.ullmatcher.ha.standby.StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());
        private final GrpcReplicationServer server = new GrpcReplicationServer(
                GrpcReplicationServerConfig.defaults(0),
                new io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationService(
                        () -> standby,
                        () -> new io.github.ike.ullmatcher.ha.state.NodeControlState(
                                "standby-a",
                                HaRole.STANDBY,
                                new io.github.ike.ullmatcher.ha.coordination.FencingToken(1L),
                                false,
                                io.github.ike.ullmatcher.runtime.MatchLoopState.RUNNING,
                                standby.cursor().lastAppliedSequence(),
                                standby.cursor()
                        ),
                        () -> new io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial(
                                java.nio.file.Files.createTempFile("snapshot", ".snap"), 0L, 0L, 0L
                        ),
                        new GrpcTransportMetrics(),
                        TimeUnit.SECONDS.toNanos(1)
                )
        );

        private StandbyFixture(String ignoredNodeId) throws IOException {
            server.start();
        }

        private int port() {
            return server.port();
        }

        @Override
        public void close() throws IOException {
            server.close();
            loop.stop();
            try {
                loopThread.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping standby fixture", e);
            }
        }
    }

    private static final class RecordingWalWriter implements WalWriter {
        private int appendCount;

        @Override
        public void append(Command command) {
            appendCount++;
        }

        @Override
        public void force() {}

        @Override
        public void close() {}
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }

    private static final class ControlledTransportProvider implements ReplicationTransportProvider {
        private final ReplicationTransportType type;
        private final Map<String, ControlledAsyncTarget> targets;

        private ControlledTransportProvider(Map<String, ControlledAsyncTarget> targets) {
            this(ReplicationTransportType.GRPC, targets);
        }

        private ControlledTransportProvider(ReplicationTransportType type, Map<String, ControlledAsyncTarget> targets) {
            this.type = type;
            this.targets = targets;
        }

        @Override
        public ReplicationTransportType type() {
            return type;
        }

        @Override
        public Map<String, String> localNodeMetadata() {
            return Map.of();
        }

        @Override
        public ClusterPeerClient connect(DiscoveredNode node) {
            return targets.get(node.nodeId());
        }

        @Override
        public TransportMetricsSnapshot metricsSnapshot() {
            return TransportMetricsSnapshot.none("GRPC");
        }

        @Override
        public void close() {}
    }

    private static final class ControlledAsyncTarget implements ClusterPeerClient, AsyncBatchReplicationTarget {
        private final String nodeId;
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        private ControlledAsyncTarget(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public String nodeId() {
            return nodeId;
        }

        @Override
        public void replicate(Command command, long timeoutNanos) {}

        @Override
        public void replicateBatch(List<Command> commands, long timeoutNanos) {}

        @Override
        public CompletableFuture<Void> replicateBatchAsync(List<Command> commands, long timeoutNanos) {
            return future;
        }

        void succeed() {
            future.complete(null);
        }

        @Override
        public io.github.ike.ullmatcher.ha.state.NodeControlState fetchNodeState(long timeoutNanos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncResult downloadLatestSnapshot(java.nio.file.Path destination, long timeoutNanos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
