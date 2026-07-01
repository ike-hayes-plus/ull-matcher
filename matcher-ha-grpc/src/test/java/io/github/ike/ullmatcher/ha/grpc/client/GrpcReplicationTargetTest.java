package io.github.ike.ullmatcher.ha.grpc.client;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationService;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GrpcReplicationTargetTest {
    private static final long TEST_TIMEOUT_NANOS = 100_000_000L;

    @Test
    void replicateAndFetchCursorRoundTrip() throws Exception {
        RecordingWalWriter wal = new RecordingWalWriter();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread loopThread = Thread.ofPlatform().start(loop);
        StandbySyncService standby = new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());

        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new GrpcReplicationService(
                        standby,
                        () -> standbyState(standby),
                        GrpcReplicationTargetTest::emptySnapshot
                ))
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try (GrpcReplicationTarget target = new GrpcReplicationTarget("standby-a", channel)) {
            target.replicate(command(1L), TEST_TIMEOUT_NANOS);
            awaitApplied(standby, 1L);

            assertEquals(1, wal.appendCount);
            assertEquals(new ReplicationCursor(1L, 1L, 1L, 0L), target.fetchCursor(TEST_TIMEOUT_NANOS));
        } finally {
            standby.close();
            loop.stop();
            loopThread.join(5_000L);
            server.shutdownNow();
        }
    }

    @Test
    void remoteStandbyFailureMapsToIoException() throws Exception {
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread loopThread = Thread.ofPlatform().start(loop);
        StandbySyncService standby = new StandbySyncService("standby-a", new FailingWalWriter(), ring, matcher, StandbySyncConfig.defaults());

        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new GrpcReplicationService(
                        standby,
                        () -> standbyState(standby),
                        GrpcReplicationTargetTest::emptySnapshot
                ))
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try (GrpcReplicationTarget target = new GrpcReplicationTarget("standby-a", channel)) {
            IOException error = assertThrows(IOException.class, () -> target.replicate(command(1L), TEST_TIMEOUT_NANOS));
            assertTrue(error.getMessage().contains("INTERNAL"));
        } finally {
            standby.close();
            loop.stop();
            loopThread.join(5_000L);
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void replicateBatchStreamsCommandsAndUpdatesCursor() throws Exception {
        RecordingWalWriter wal = new RecordingWalWriter();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread loopThread = Thread.ofPlatform().start(loop);
        StandbySyncService standby = new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());

        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new GrpcReplicationService(
                        standby,
                        () -> standbyState(standby),
                        GrpcReplicationTargetTest::emptySnapshot
                ))
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try (GrpcReplicationTarget target = new GrpcReplicationTarget("standby-a", channel)) {
            target.replicateBatch(List.of(command(1L), command(2L), command(3L)), TEST_TIMEOUT_NANOS);
            awaitApplied(standby, 3L);

            assertEquals(3, wal.appendCount);
            assertEquals(1, wal.forceCount);
            assertEquals(new ReplicationCursor(3L, 3L, 3L, 0L), target.fetchCursor(TEST_TIMEOUT_NANOS));
        } finally {
            standby.close();
            loop.stop();
            loopThread.join(5_000L);
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void largeReplicationBatchUsesConfiguredStreamBatching() throws Exception {
        RecordingWalWriter wal = new RecordingWalWriter();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(2_048);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        MatchLoop loop = new MatchLoop(ring, matcher);
        Thread loopThread = Thread.ofPlatform().start(loop);
        StandbySyncService standby = new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());

        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new GrpcReplicationService(
                        standby,
                        () -> standbyState(standby),
                        GrpcReplicationTargetTest::emptySnapshot
                ))
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try (GrpcReplicationTarget target = new GrpcReplicationTarget("standby-a", channel)) {
            java.util.ArrayList<Command> commands = new java.util.ArrayList<>(600);
            for (long sequence = 1L; sequence <= 600L; sequence++) {
                commands.add(command(sequence));
            }
            target.replicateBatch(commands, TEST_TIMEOUT_NANOS);
            awaitApplied(standby, 600L);

            assertEquals(600, wal.appendCount);
            assertEquals(3, wal.forceCount);
            assertEquals(new ReplicationCursor(600L, 600L, 600L, 0L), target.fetchCursor(TEST_TIMEOUT_NANOS));
        } finally {
            standby.close();
            loop.stop();
            loopThread.join(5_000L);
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void replicationIsRejectedWhenStandbyIngressIsPaused() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new GrpcReplicationService(
                        () -> {
                            throw new IllegalStateException("standby replication ingress is paused");
                        },
                        () -> new NodeControlState(
                                "standby-a",
                                HaRole.STANDBY,
                                new FencingToken(1L),
                                false,
                                MatchLoopState.QUIESCING,
                                0L,
                                new ReplicationCursor(0L, 0L, 0L, 0L)
                        ),
                        GrpcReplicationTargetTest::emptySnapshot,
                        new GrpcTransportMetrics()
                ))
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try (GrpcReplicationTarget target = new GrpcReplicationTarget("standby-a", channel)) {
            IOException error = assertThrows(IOException.class, () -> target.replicate(command(3L), TEST_TIMEOUT_NANOS));
            assertTrue(error.getMessage().contains("FAILED_PRECONDITION"));
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 1_000L + sequence, 2_000L + sequence, 1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
    }

    private static NodeControlState standbyState(StandbySyncService standby) {
        return new NodeControlState(
                standby.nodeId(),
                HaRole.STANDBY,
                new FencingToken(1L),
                false,
                MatchLoopState.RUNNING,
                standby.cursor().lastAppliedSequence(),
                standby.cursor()
        );
    }

    private static io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial emptySnapshot() throws IOException {
        return new io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial(Files.createTempFile("snapshot", ".snap"), 0L, 0L, 0L);
    }

    private static void awaitApplied(StandbySyncService standby, long sequence) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline && standby.cursor().lastAppliedSequence() < sequence) {
            Thread.sleep(10L);
        }
        assertEquals(sequence, standby.cursor().lastAppliedSequence());
    }

    private static final class RecordingWalWriter implements WalWriter {
        private int appendCount;
        private int forceCount;

        @Override
        public void append(Command command) {
            appendCount++;
        }

        @Override
        public void force() {
            forceCount++;
        }

        @Override
        public void close() {}
    }

    private static final class FailingWalWriter implements WalWriter {
        @Override
        public void append(Command command) throws IOException {
            throw new IOException("disk full");
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
}
