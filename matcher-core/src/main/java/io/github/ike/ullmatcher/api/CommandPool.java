package io.github.ike.ullmatcher.api;

import io.github.ike.ullmatcher.queue.MpscArrayQueue;

/**
 * 预分配命令槽位池。
 * <p>
 * 用于客户端提交流水线，避免在热路径上为每笔订单和撤单分配新对象。
 */
public final class CommandPool implements Command.Recycler {
    private final MpscArrayQueue<Command> available;

    public CommandPool(int capacityPowerOfTwo) {
        this.available = new MpscArrayQueue<>(capacityPowerOfTwo);
        for (int i = 0; i < capacityPowerOfTwo; i++) {
            if (!available.offer(Command.pooled(this))) {
                throw new IllegalStateException("failed to initialize command pool");
            }
        }
    }

    public Command borrowNewOrder(long sequence,
                                  long orderId,
                                  long userId,
                                  int symbolId,
                                  Side side,
                                  OrderType orderType,
                                  TimeInForce tif,
                                  long price,
                                  long quantity,
                                  long expireAtEpochMillis) {
        Command command = available.poll();
        if (command == null) {
            return null;
        }
        return command.prepareNewOrder(sequence, orderId, userId, symbolId, side, orderType, tif, price, quantity, expireAtEpochMillis);
    }

    public Command borrowCancel(long sequence, long orderId, int symbolId) {
        Command command = available.poll();
        if (command == null) {
            return null;
        }
        return command.prepareCancel(sequence, orderId, symbolId);
    }

    public int available() {
        return available.size();
    }

    public int capacity() {
        return available.capacity();
    }

    @Override
    public void recycle(Command command) {
        while (!available.offer(command)) {
            Thread.onSpinWait();
        }
    }
}
