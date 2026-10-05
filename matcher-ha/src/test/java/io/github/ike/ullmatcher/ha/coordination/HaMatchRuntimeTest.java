package io.github.ike.ullmatcher.ha.coordination;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runtime is the only thing that opens and closes the local match loop to clients, so each role
 * transition is checked against both the role field and the loop's accept flag. Fencing is terminal:
 * once fenced, no transition may reopen the loop.
 */
final class HaMatchRuntimeTest {
    @Test
    void aPrimaryRuntimeStartsOpenForClientCommands() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(1L));

        assertEquals("node-a", runtime.nodeId());
        assertEquals(HaRole.PRIMARY, runtime.role());
        assertEquals(new FencingToken(1L), runtime.fencingToken());
        assertTrue(runtime.acceptsClientCommands());
        assertTrue(loop.isAcceptingCommands());
    }

    @Test
    void standbyAndCatchingUpRuntimesStartQuiescedButAlive() {
        MatchLoop standbyLoop = newLoop();
        MatchLoop catchUpLoop = newLoop();

        HaMatchRuntime standby = new HaMatchRuntime("node-b", standbyLoop, HaRole.STANDBY, new FencingToken(1L));
        HaMatchRuntime catchingUp =
                new HaMatchRuntime("node-c", catchUpLoop, HaRole.CATCHING_UP, new FencingToken(1L));

        assertFalse(standby.acceptsClientCommands());
        assertFalse(catchingUp.acceptsClientCommands());
        assertTrue(standbyLoop.isRunning());
        assertTrue(catchUpLoop.isRunning());
        assertEquals(MatchLoopState.QUIESCING, standbyLoop.state());
        assertEquals(MatchLoopState.QUIESCING, catchUpLoop.state());
    }

    @Test
    void aRuntimeConstructedAsFencedStopsTheLoopImmediately() {
        MatchLoop loop = newLoop();

        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.FENCED, new FencingToken(1L));

        assertEquals(HaRole.FENCED, runtime.role());
        assertFalse(runtime.acceptsClientCommands());
        assertFalse(loop.isRunning());
        assertEquals(MatchLoopState.STOPPED, loop.state());
    }

    @Test
    void promotionOpensTheLoopAndAdoptsTheNewFencingToken() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-b", loop, HaRole.STANDBY, new FencingToken(1L));

        runtime.promote(new FencingToken(5L));

        assertEquals(HaRole.PRIMARY, runtime.role());
        assertEquals(new FencingToken(5L), runtime.fencingToken());
        assertTrue(runtime.acceptsClientCommands());
        assertEquals(MatchLoopState.RUNNING, loop.state());
    }

    @Test
    void demotionClosesTheLoopButKeepsTheFencingTokenForLeaseChecks() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(4L));

        runtime.demoteToStandby();

        assertEquals(HaRole.STANDBY, runtime.role());
        assertEquals(new FencingToken(4L), runtime.fencingToken());
        assertFalse(runtime.acceptsClientCommands());
        assertTrue(loop.isRunning(), "a demoted node keeps draining its loop");
        assertEquals(MatchLoopState.QUIESCING, loop.state());
    }

    @Test
    void catchUpClosesTheLoopToClientsWithoutStoppingIt() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(1L));

        runtime.beginCatchUp();

        assertEquals(HaRole.CATCHING_UP, runtime.role());
        assertFalse(runtime.acceptsClientCommands());
        assertTrue(loop.isRunning());
        assertEquals(MatchLoopState.QUIESCING, loop.state());
    }

    @Test
    void demoteThenPromoteRoundTripsBackToServingClients() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(1L));

        runtime.demoteToStandby();
        runtime.beginCatchUp();
        runtime.promote(new FencingToken(2L));

        assertEquals(HaRole.PRIMARY, runtime.role());
        assertEquals(new FencingToken(2L), runtime.fencingToken());
        assertTrue(runtime.acceptsClientCommands());
    }

    @Test
    void repeatedPromotionIsIdempotentApartFromTheEpoch() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.STANDBY, new FencingToken(1L));

        runtime.promote(new FencingToken(2L));
        runtime.promote(new FencingToken(3L));

        assertEquals(HaRole.PRIMARY, runtime.role());
        assertEquals(new FencingToken(3L), runtime.fencingToken());
        assertTrue(runtime.acceptsClientCommands());
    }

    @Test
    void fencingIsTerminalAndEveryTransitionAwayFromItIsRejected() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(1L));

        runtime.fence();

        assertEquals(HaRole.FENCED, runtime.role());
        assertFalse(runtime.acceptsClientCommands());
        assertFalse(loop.isRunning());
        assertEquals("cannot promote fenced runtime",
                assertThrows(IllegalStateException.class, () -> runtime.promote(new FencingToken(9L))).getMessage());
        assertEquals("cannot demote fenced runtime",
                assertThrows(IllegalStateException.class, runtime::demoteToStandby).getMessage());
        assertEquals("cannot catch up fenced runtime",
                assertThrows(IllegalStateException.class, runtime::beginCatchUp).getMessage());
        assertEquals(HaRole.FENCED, runtime.role(), "a rejected transition must not change the role");
        assertEquals(new FencingToken(1L), runtime.fencingToken());
    }

    @Test
    void fencingTwiceIsIdempotent() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(1L));

        runtime.fence();
        runtime.fence();

        assertEquals(HaRole.FENCED, runtime.role());
        assertFalse(loop.isRunning());
    }

    @Test
    void snapshotReportsTheUnderlyingLoopState() {
        MatchLoop loop = newLoop();
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", loop, HaRole.PRIMARY, new FencingToken(1L));

        assertTrue(runtime.snapshot().acceptingCommands());
        assertEquals(0L, runtime.snapshot().processedCommandCount());

        runtime.fence();

        assertFalse(runtime.snapshot().acceptingCommands());
        assertEquals(MatchLoopState.STOPPED, runtime.snapshot().state());
    }

    @Test
    void promotionRequiresANonNullToken() {
        HaMatchRuntime runtime = new HaMatchRuntime("node-a", newLoop(), HaRole.STANDBY, new FencingToken(1L));

        assertThrows(NullPointerException.class, () -> runtime.promote(null));
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        MatchLoop loop = newLoop();

        assertThrows(NullPointerException.class,
                () -> new HaMatchRuntime(null, loop, HaRole.STANDBY, new FencingToken(1L)));
        assertThrows(NullPointerException.class,
                () -> new HaMatchRuntime("node-a", null, HaRole.STANDBY, new FencingToken(1L)));
        assertThrows(NullPointerException.class,
                () -> new HaMatchRuntime("node-a", loop, null, new FencingToken(1L)));
        assertThrows(NullPointerException.class,
                () -> new HaMatchRuntime("node-a", loop, HaRole.STANDBY, null));
    }

    private static MatchLoop newLoop() {
        return new MatchLoop(
                new SpscRingBuffer<>(16),
                new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler()));
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }
}
