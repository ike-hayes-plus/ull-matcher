package io.github.ike.ullmatcher.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CommandPoolTest {
    @Test
    void borrowedSlotsCarryTheRequestedFieldsAndReturnToThePoolOnRelease() {
        CommandPool pool = new CommandPool(4);
        assertEquals(4, pool.capacity());
        assertEquals(4, pool.available());

        Command command = pool.borrowNewOrder(1L, 10L, 20L, 7, Side.BUY,
                OrderType.LIMIT, TimeInForce.GTC, 100L, 5L, 1_700_000_000_000L);

        assertNotNull(command);
        assertEquals(CommandType.NEW_ORDER, command.type);
        assertEquals(1L, command.sequence);
        assertEquals(10L, command.orderId);
        assertEquals(20L, command.userId);
        assertEquals(7, command.symbolId);
        assertEquals(Side.BUY.code, command.side);
        assertEquals(OrderType.LIMIT.code, command.orderType);
        assertEquals(TimeInForce.GTC.code, command.timeInForce);
        assertEquals(100L, command.price);
        assertEquals(5L, command.quantity);
        assertEquals(1_700_000_000_000L, command.expireAtEpochMillis);
        assertEquals(3, pool.available());

        command.release();

        assertEquals(4, pool.available());
    }

    @Test
    void cancelSlotsClearOrderSpecificFields() {
        CommandPool pool = new CommandPool(2);

        Command command = pool.borrowCancel(9L, 11L, 3);

        assertEquals(CommandType.CANCEL_ORDER, command.type);
        assertEquals(9L, command.sequence);
        assertEquals(11L, command.orderId);
        assertEquals(3, command.symbolId);
        assertEquals(0L, command.userId);
        assertEquals(0L, command.price);
        assertEquals(0L, command.quantity);
    }

    @Test
    void exhaustedPoolReturnsNullInsteadOfAllocating() {
        CommandPool pool = new CommandPool(2);

        assertNotNull(pool.borrowCancel(1L, 1L, 1));
        assertNotNull(pool.borrowCancel(2L, 2L, 1));

        assertNull(pool.borrowCancel(3L, 3L, 1));
        assertNull(pool.borrowNewOrder(3L, 3L, 1L, 1, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 1L, 1L, 0L));
        assertEquals(0, pool.available());
    }

    @Test
    void retainDefersRecyclingUntilTheLastReferenceIsReleased() {
        CommandPool pool = new CommandPool(1);
        Command command = pool.borrowCancel(1L, 1L, 1);

        assertSame(command, command.retain());
        command.release();
        assertEquals(0, pool.available(), "slot must stay out while a reference is held");

        command.release();
        assertEquals(1, pool.available());
    }

    @Test
    void releasedSlotIsClearedBeforeReuse() {
        CommandPool pool = new CommandPool(1);
        Command borrowed = pool.borrowNewOrder(1L, 10L, 20L, 7, Side.BUY,
                OrderType.LIMIT, TimeInForce.GTC, 100L, 5L, 42L);
        borrowed.release();

        Command reused = pool.borrowCancel(2L, 11L, 3);

        assertSame(borrowed, reused, "pool must hand back the same slot");
        assertEquals(0L, reused.userId);
        assertEquals(0L, reused.price);
        assertEquals(0L, reused.expireAtEpochMillis);
    }

    @Test
    void doubleReleaseOfAPooledSlotFailsFast() {
        CommandPool pool = new CommandPool(1);
        Command command = pool.borrowCancel(1L, 1L, 1);
        command.release();

        assertThrows(IllegalStateException.class, command::release);
    }

    @Test
    void retainingAReleasedSlotFailsFast() {
        CommandPool pool = new CommandPool(1);
        Command command = pool.borrowCancel(1L, 1L, 1);
        command.release();

        assertThrows(IllegalStateException.class, command::retain);
    }

    @Test
    void standaloneCommandsIgnoreReferenceCounting() {
        Command command = Command.newOrder(1L, 2L, 3L, 4, Side.SELL,
                OrderType.MARKET_WITH_PROTECTION, TimeInForce.IOC, 50L, 2L);

        assertSame(command, command.retain());
        command.release();
        command.release();

        assertEquals(CommandType.NEW_ORDER, command.type, "standalone commands are never cleared");
    }

    @Test
    void pooledSlotWithoutRecyclerBehavesAsStandalone() {
        Command command = Command.pooled(null);

        assertSame(command, command.retain());
        command.release();
    }
}
