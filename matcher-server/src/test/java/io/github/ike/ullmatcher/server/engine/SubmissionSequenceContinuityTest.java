package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandPool;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaMatchRuntime;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.hft.JournaledMatcherGateway;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class SubmissionSequenceContinuityTest {
    @TempDir
    Path dir;

    @Test
    void preWalRejectionDoesNotConsumeSequence() throws Exception {
        AtomicLong nextSequence = new AtomicLong(1L);
        SubmissionCoordinator coordinator = coordinator(nextSequence, 4);
        SpscRingBuffer<Command> gatewayRing = new SpscRingBuffer<>(2);
        assertEquals(true, gatewayRing.offer(Command.shutdown(90L)));
        assertEquals(true, gatewayRing.offer(Command.shutdown(91L)));
        MatcherEngine engine = engine(gatewayRing);

        SubmissionCoordinator.BatchSubmitContext context = new SubmissionCoordinator.BatchSubmitContext(1);
        coordinator.submitBatch(engine, List.of(order(1L)), context, false);

        assertEquals(SubmitResult.RING_FULL_BEFORE_WAL_APPEND, context.result);
        assertEquals(1L, nextSequence.get());

        assertEquals(90L, gatewayRing.poll().sequence);
        assertEquals(91L, gatewayRing.poll().sequence);
        SubmissionCoordinator.BatchSubmitContext accepted = new SubmissionCoordinator.BatchSubmitContext(1);
        coordinator.submitBatch(engine, List.of(order(2L)), accepted, false);

        assertEquals(SubmitResult.ACCEPTED, accepted.result);
        assertEquals(1L, accepted.prepared[0].sequence());
        assertEquals(2L, nextSequence.get());
    }

    @Test
    void commandPoolExhaustionRollsSequenceBack() throws Exception {
        AtomicLong nextSequence = new AtomicLong(1L);
        SubmissionCoordinator coordinator = coordinator(nextSequence, 2);
        MatcherEngine engine = engine(new SpscRingBuffer<>(8));
        SubmissionCoordinator.BatchSubmitContext context = new SubmissionCoordinator.BatchSubmitContext(4);

        coordinator.submitBatch(engine, List.of(order(1L), order(2L), order(3L)), context, true);

        assertEquals(SubmitResult.COMMAND_POOL_EXHAUSTED, context.result);
        assertEquals(1L, nextSequence.get());
    }

    private SubmissionCoordinator coordinator(AtomicLong nextSequence, int poolCapacity) {
        MatcherServerConfig config = MatcherServerConfig.builder("node-a", 1, dir)
                .gatewayOfferTimeoutNanos(0L)
                .build();
        return new SubmissionCoordinator(
                config,
                new CommandPool(poolCapacity),
                nextSequence,
                new TtlCancelGuard(TtlCancelConfig.disabled(), orderId -> SubmitResult.ACCEPTED, 1),
                new OrderStateTracker(8)
        );
    }

    private static MatcherEngine engine(SpscRingBuffer<Command> gatewayRing) {
        MatchLoop loop = new MatchLoop(
                new SpscRingBuffer<>(8),
                new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopEvents())
        );
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(1L));
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                new MemoryWal(),
                gatewayRing,
                1,
                0L,
                () -> true,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L
        );
        return new MatcherEngine(null, gatewayRing, null, loop, runtime, new Thread(), gateway, null);
    }

    private static SubmissionRequest.NewOrderRequest order(long orderId) {
        return new SubmissionRequest.NewOrderRequest(
                7L, orderId, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L, null);
    }

    private static final class MemoryWal implements WalWriter {
        @Override
        public void append(Command command) {
        }

        @Override
        public void force() {
        }

        @Override
        public void close() {
        }
    }

    private static final class NoopEvents implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {
        }

        @Override
        public void onOrder(OrderEvent event) {
        }
    }
}
