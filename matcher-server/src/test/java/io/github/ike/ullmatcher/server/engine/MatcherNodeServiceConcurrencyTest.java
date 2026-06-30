package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.readiness.PromotionReadinessPolicy;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.replication.ReplicationResult;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.cluster.AeronPreviewTransportConfig;
import io.github.ike.ullmatcher.server.cluster.MatcherClusterConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportPolicyConfig;
import io.github.ike.ullmatcher.server.cluster.ReplicationTransportType;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherNodeServiceConcurrencyTest {
    @Test
    void concurrentClientSubmissionsRemainCorrectBehindSingleProducerControlThread() throws Exception {
        Path dir = Files.createTempDirectory("matcher-node-concurrency");
        MatcherServerConfig config = testConfig(dir);
        try (MatcherNodeService service = new MatcherNodeService(config)) {
            service.start();
            assertTrue(await(() -> service.health().acceptingClientCommands(), 5_000L));
            Thread.sleep(50L);
            int orderCount = 64;
            ExecutorService clients = Executors.newFixedThreadPool(8);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<java.util.concurrent.Future<MatcherNodeService.SubmitResponse>> futures = new ArrayList<>();
                for (int i = 0; i < orderCount; i++) {
                    final long orderId = 10_000L + i;
                    futures.add(clients.submit(() -> {
                        start.await();
                        return service.submitNewOrder(1L, orderId, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
                    }));
                }
                start.countDown();
                for (var future : futures) {
                    assertEquals(io.github.ike.ullmatcher.hft.SubmitResult.ACCEPTED, future.get(5, TimeUnit.SECONDS).result());
                }
            } finally {
                clients.shutdownNow();
                assertTrue(clients.awaitTermination(5, TimeUnit.SECONDS));
            }
            assertTrue(await(() -> service.liveOrderCount() == orderCount, 5_000L));
        }
    }

    @Test
    void batchSubmitPreservesEnvelopeSizeForReplication() throws Exception {
        Path dir = Files.createTempDirectory("matcher-node-batch-envelope");
        MatcherServerConfig config = testConfig(dir);
        RecordingReplicator replicator = new RecordingReplicator();
        try (MatcherNodeService service = new MatcherNodeService(config)) {
            service.start();
            service.configureReplication(replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TimeUnit.SECONDS.toNanos(1));

            List<MatcherNodeService.BatchNewOrderRequest> batch = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                batch.add(new MatcherNodeService.BatchNewOrderRequest(
                        1L,
                        20_000L + i,
                        Side.BUY,
                        OrderType.LIMIT,
                        TimeInForce.GTC,
                        100L,
                        1L,
                        null
                ));
            }

            List<MatcherNodeService.SubmitResponse> responses = service.submitNewOrderBatch(batch);
            assertEquals(64, responses.size());
            for (MatcherNodeService.SubmitResponse response : responses) {
                assertEquals(io.github.ike.ullmatcher.hft.SubmitResult.ACCEPTED, response.result());
            }

            assertTrue(await(() -> replicator.invocations.get() == 1, 5_000L));
            assertEquals(List.of(64), replicator.batchSizes);
        }
    }

    @Test
    void concurrentBatchSubmissionsAreNotPartiallySplitAcrossReplicationBatches() throws Exception {
        Path dir = Files.createTempDirectory("matcher-node-batch-deferred");
        MatcherServerConfig config = testConfig(dir);
        RecordingReplicator replicator = new RecordingReplicator();
        try (MatcherNodeService service = new MatcherNodeService(config)) {
            service.start();
            service.configureReplication(replicator, ReplicationMode.WAIT_FOR_ANY_STANDBY, TimeUnit.SECONDS.toNanos(1));

            ExecutorService clients = Executors.newFixedThreadPool(2);
            try {
                CountDownLatch start = new CountDownLatch(1);
                CompletableFuture<List<MatcherNodeService.SubmitResponse>> firstFuture = new CompletableFuture<>();
                CompletableFuture<List<MatcherNodeService.SubmitResponse>> secondFuture = new CompletableFuture<>();

                clients.submit(() -> {
                    try {
                        start.await();
                        firstFuture.complete(service.submitNewOrderBatch(newOrderBatch(30_000L, 200)));
                    } catch (Throwable error) {
                        firstFuture.completeExceptionally(error);
                    }
                });
                clients.submit(() -> {
                    try {
                        start.await();
                        secondFuture.complete(service.submitNewOrderBatch(newOrderBatch(40_000L, 200)));
                    } catch (Throwable error) {
                        secondFuture.completeExceptionally(error);
                    }
                });
                start.countDown();

                assertEquals(200, firstFuture.get(5, TimeUnit.SECONDS).size());
                assertEquals(200, secondFuture.get(5, TimeUnit.SECONDS).size());
            } finally {
                clients.shutdownNow();
                assertTrue(clients.awaitTermination(5, TimeUnit.SECONDS));
            }

            assertTrue(await(() -> replicator.invocations.get() == 2, 5_000L));
            assertEquals(List.of(200, 200), replicator.batchSizes);
        }
    }

    @Test
    void primarySubmitFencesLocalRuntimeWhenLeaseBelongsToRecoveredNewPrimary() throws Exception {
        Path dir = Files.createTempDirectory("matcher-node-submit-lease-fence");
        MutableLeaseStore leaseStore = new MutableLeaseStore(
                new ClusterLease("node-b", new FencingToken(2L), System.nanoTime() + TimeUnit.SECONDS.toNanos(30))
        );
        MatcherServerConfig config = testConfigWithCluster(dir, leaseStore);
        try (MatcherNodeService service = new MatcherNodeService(config)) {
            service.start();

            IOException error = assertThrows(IOException.class, () -> service.submitNewOrder(
                    1L, 50_000L, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L
            ));

            assertTrue(error.getMessage().contains("primary lease is not held"));
            assertEquals(HaRole.FENCED, service.currentState().role());
            assertFalse(service.currentState().acceptingClientCommands());
        }
    }

    private static List<MatcherNodeService.BatchNewOrderRequest> newOrderBatch(long orderIdStart, int count) {
        ArrayList<MatcherNodeService.BatchNewOrderRequest> requests = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            requests.add(new MatcherNodeService.BatchNewOrderRequest(
                    1L,
                    orderIdStart + i,
                    Side.BUY,
                    OrderType.LIMIT,
                    TimeInForce.GTC,
                    100L,
                    1L,
                    null
            ));
        }
        return requests;
    }

    private static MatcherServerConfig testConfig(Path dir) {
        return new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "symbol-1",
                MatcherConfig.defaults(1),
                dir.resolve("wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                dir.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 10,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                0,
                "127.0.0.1",
                4,
                1 << 20,
                256,
                2_000L,
                128,
                96,
                16,
                2_000L,
                1_000L,
                5_000L,
                96,
                64,
                2,
                16,
                8,
                WriteAdmissionPolicyConfig.defaults(),
                false,
                0,
                GrpcReplicationServerConfig.defaults(0),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );
    }

    private static MatcherServerConfig testConfigWithCluster(Path dir, LeaseStore leaseStore) {
        MatcherClusterConfig clusterConfig = new MatcherClusterConfig(
                leaseStore,
                new InMemoryNodeRegistry(),
                "symbol-1",
                "127.0.0.1",
                25L,
                TimeUnit.MILLISECONDS.toNanos(100),
                TimeUnit.SECONDS.toNanos(5),
                FailoverPolicy.defaults(),
                PromotionReadinessPolicy.strict(),
                0L,
                TimeUnit.SECONDS.toNanos(1),
                ReplicationMode.LOCAL_ONLY,
                TimeUnit.MILLISECONDS.toNanos(50),
                ReplicationTransportType.GRPC,
                new AeronPreviewTransportConfig(dir.resolve("aeron-preview"), 15_290, 11_191),
                ReplicationTransportPolicyConfig.defaults()
        );
        return new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "symbol-1",
                MatcherConfig.defaults(1),
                dir.resolve("wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                dir.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 10,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                0,
                "127.0.0.1",
                4,
                1 << 20,
                256,
                2_000L,
                128,
                96,
                16,
                2_000L,
                1_000L,
                5_000L,
                96,
                64,
                2,
                16,
                8,
                WriteAdmissionPolicyConfig.defaults(),
                false,
                0,
                GrpcReplicationServerConfig.defaults(0),
                ServerSecurityConfig.insecureDefaults(),
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                clusterConfig
        );
    }

    private static boolean await(Check check, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (check.ok()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    private static final class RecordingReplicator implements CommandReplicator {
        private final List<Integer> batchSizes = java.util.Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public io.github.ike.ullmatcher.ha.replication.ReplicationResult replicate(io.github.ike.ullmatcher.api.Command command, long timeoutNanos) {
            throw new UnsupportedOperationException("single-command path is not used in this test");
        }

        @Override
        public synchronized CompletableFuture<ReplicationResult> replicateBatchAsync(List<io.github.ike.ullmatcher.api.Command> commands, long timeoutNanos) {
            batchSizes.add(commands.size());
            invocations.incrementAndGet();
            return CompletableFuture.completedFuture(new ReplicationResult(1, 1, List.of("standby-a"), List.of()));
        }
    }

    private static final class InMemoryNodeRegistry implements NodeRegistry {
        private final List<DiscoveredNode> nodes = new ArrayList<>();

        @Override
        public synchronized void registerOrUpdate(DiscoveredNode node) {
            nodes.removeIf(existing -> existing.nodeId().equals(node.nodeId()));
            nodes.add(node);
        }

        @Override
        public synchronized void unregister(String nodeId) {
            nodes.removeIf(node -> node.nodeId().equals(nodeId));
        }

        @Override
        public synchronized List<DiscoveredNode> listNodes() {
            return new ArrayList<>(nodes);
        }
    }

    private static final class MutableLeaseStore implements LeaseStore {
        private volatile ClusterLease lease;

        private MutableLeaseStore(ClusterLease lease) {
            this.lease = lease;
        }

        @Override
        public ClusterLease currentLease() {
            return lease;
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (lease == null || lease.isExpired(nowNanos) || lease.ownerNodeId().equals(nodeId)) {
                lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
                return true;
            }
            return false;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (lease != null
                    && !lease.isExpired(nowNanos)
                    && lease.ownerNodeId().equals(nodeId)
                    && lease.fencingToken().equals(fencingToken)) {
                lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
                return true;
            }
            return false;
        }
    }
}
