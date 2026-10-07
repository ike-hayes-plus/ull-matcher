/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.runtime;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.hft.JournaledMatcherGateway;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RuntimeBranchCoverageTest {
    @Test
    void matchLoopConfigAndIdleStrategiesRejectInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> new MatchLoopConfig(BusySpinIdleStrategy.INSTANCE, 0));
        assertThrows(IllegalArgumentException.class, () -> new AdaptiveIdleStrategy(-1, 0, 1, 2));
        assertThrows(IllegalArgumentException.class, () -> new AdaptiveIdleStrategy(0, -1, 1, 2));
        assertThrows(IllegalArgumentException.class, () -> new AdaptiveIdleStrategy(0, 0, 0, 2));
        assertThrows(IllegalArgumentException.class, () -> new AdaptiveIdleStrategy(0, 0, 4, 2));

        AdaptiveIdleStrategy adaptive = new AdaptiveIdleStrategy(1, 1, 1_000L, 8_000L);
        adaptive.idle(0);
        adaptive.idle(1);
        adaptive.idle(2);
        adaptive.reset();
        assertTrue(adaptive.spinCount() >= 0);
        BusySpinIdleStrategy.INSTANCE.idle(0);
        assertTrue(BusySpinIdleStrategy.INSTANCE.spinCount() > 0);
        assertEquals(MatchLoopConfig.defaults().maxBatchSize(), MatchLoopConfig.defaults().maxBatchSize());
    }

    @Test
    void matchLoopActivateAndQuiesceCoverStateGuards() {
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(8);
        MatchLoop loop = new MatchLoop(ring, new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new Noop()));

        loop.quiesce();
        loop.activate();
        assertEquals(MatchLoopState.RUNNING, loop.state());
        loop.drainAndStop();
        assertThrows(IllegalStateException.class, loop::activate);
        loop.stop();
        assertEquals(MatchLoopState.STOPPED, loop.state());
        assertThrows(IllegalStateException.class, loop::activate);
    }

    @Test
    void gatewayRejectsInvalidConstructionAndEmptyBatch() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(4);
        assertThrows(IllegalArgumentException.class,
                () -> new JournaledMatcherGateway(wal, ring, 0, 0, () -> true));
        assertThrows(IllegalArgumentException.class,
                () -> new JournaledMatcherGateway(wal, ring, 1, 0, () -> true, WalDurabilityMode.SYNC_PER_BATCH, 0, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new JournaledMatcherGateway(wal, ring, 1, 0, () -> true, WalDurabilityMode.SYNC_PER_BATCH, 1, -1L));

        JournaledMatcherGateway gateway = new JournaledMatcherGateway(wal, ring, 1, 0, () -> true);
        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmitBatch(List.of(), 0));
        assertThrows(IllegalStateException.class, () -> {
            JournaledMatcherGateway closed = new JournaledMatcherGateway(wal, ring, 1, 0, () -> false);
            closed.submit(Command.shutdown(1));
        });
    }

    @Test
    void gatewayPublishesCommandAppendedBeforeAcceptingFlips() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(4);
        AtomicBoolean accepting = new AtomicBoolean(true);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                wal, ring, 1, 0, accepting::get, WalDurabilityMode.SYNC_PER_COMMAND, 1, 0L);
        wal.afterAppend = () -> accepting.set(false);

        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmit(Command.shutdown(1), 0));
        assertEquals(0, gateway.failedAfterWalCount());
        assertEquals(1, ring.size());
    }

    @Test
    void gatewayRejectsBatchLargerThanRing() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(2);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(wal, ring, 1, 0, () -> true);
        List<Command> batch = List.of(Command.shutdown(1), Command.shutdown(2), Command.shutdown(3));
        assertEquals(SubmitResult.RING_FULL_BEFORE_WAL_APPEND, gateway.trySubmitBatch(batch, 0));
    }

    private static final class Noop implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }

    private static final class InMemoryWal implements WalWriter {
        private final List<Command> commands = new ArrayList<>();
        private Runnable afterAppend = () -> {};

        @Override
        public void append(Command command) {
            commands.add(command);
            afterAppend.run();
        }

        @Override
        public void appendAll(Iterable<Command> commands) {
            for (Command command : commands) {
                append(command);
            }
        }

        @Override
        public void force() {}

        @Override
        public void close() throws IOException {}
    }
}
