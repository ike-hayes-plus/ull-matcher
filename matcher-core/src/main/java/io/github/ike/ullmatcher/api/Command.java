package io.github.ike.ullmatcher.api;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 进入撮合分片的命令记录。
 * <p>
 * 高频交易约束：
 * <ol>
 *     <li>所有价格、数量都使用 {@code long} 定点数，禁止 {@code BigDecimal} 进入撮合热路径。</li>
 *     <li>{@link #sequence} 必须由上游定序器严格递增分配，用于幂等、恢复和重放。</li>
 *     <li>{@code Command} 是跨线程传输记录；提交热路径可复用预分配槽位，进入撮合线程后不再修改。</li>
 * </ol>
 */
public final class Command {
    private static final VarHandle REFERENCES;

    static {
        try {
            REFERENCES = MethodHandles.lookup().findVarHandle(Command.class, "references", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Recycler NOOP_RECYCLER = command -> {
    };

    /** 命令类型，决定撮合线程执行的分支。 */
    public CommandType type;

    /** 全局递增序列号，用于去重、恢复和重放顺序控制。 */
    public long sequence;

    /** 订单编号；非订单类控制命令使用 {@code 0}。 */
    public long orderId;

    /** 用户编号；控制命令和撤单命令可使用 {@code 0}。 */
    public long userId;

    /** 交易对或撮合分片编号。 */
    public int symbolId;

    /** 买卖方向编码，取值来自 {@link Side#code}。 */
    public byte side;

    /** 订单类型编码，取值来自 {@link OrderType#code}。 */
    public byte orderType;

    /** 有效期策略编码，取值来自 {@link TimeInForce#code}。 */
    public byte timeInForce;

    /** 定点数价格；市价保护单同样需要携带保护价。 */
    public long price;

    /** 定点数数量。 */
    public long quantity;

    /** 订单绝对过期时间，单位为 epoch millis；非 TTL 订单或控制命令为 {@code 0}。 */
    public long expireAtEpochMillis;

    private Recycler recycler = NOOP_RECYCLER;
    private volatile int references;

    private Command() {
    }

    public Command prepareNewOrder(long sequence, long orderId, long userId, int symbolId, Side side,
                                   OrderType orderType, TimeInForce tif, long price, long quantity) {
        return prepareNewOrder(sequence, orderId, userId, symbolId, side, orderType, tif, price, quantity, 0L);
    }

    public Command prepareNewOrder(long sequence, long orderId, long userId, int symbolId, Side side,
                                   OrderType orderType, TimeInForce tif, long price, long quantity, long expireAtEpochMillis) {
        return reset(CommandType.NEW_ORDER, sequence, orderId, userId, symbolId,
                side.code, orderType.code, tif.code, price, quantity, expireAtEpochMillis);
    }

    public Command prepareCancel(long sequence, long orderId, int symbolId) {
        return reset(CommandType.CANCEL_ORDER, sequence, orderId, 0L, symbolId,
                (byte) 0, (byte) 0, (byte) 0, 0L, 0L, 0L);
    }

    public Command prepareSnapshotMarker(long sequence, int symbolId) {
        return reset(CommandType.SNAPSHOT_MARKER, sequence, 0L, 0L, symbolId,
                (byte) 0, (byte) 0, (byte) 0, 0L, 0L, 0L);
    }

    public Command prepareShutdown(long sequence) {
        return reset(CommandType.SHUTDOWN, sequence, 0L, 0L, 0,
                (byte) 0, (byte) 0, (byte) 0, 0L, 0L, 0L);
    }

    public Command retain() {
        if (recycler == NOOP_RECYCLER) {
            return this;
        }
        while (true) {
            int current = (int) REFERENCES.getVolatile(this);
            if (current <= 0) {
                throw new IllegalStateException("command slot is not active");
            }
            if (REFERENCES.compareAndSet(this, current, current + 1)) {
                return this;
            }
        }
    }

    public void release() {
        if (recycler == NOOP_RECYCLER) {
            return;
        }
        while (true) {
            int current = (int) REFERENCES.getVolatile(this);
            if (current <= 0) {
                throw new IllegalStateException("command slot already released");
            }
            int next = current - 1;
            if (REFERENCES.compareAndSet(this, current, next)) {
                if (next == 0) {
                    clear();
                    recycler.recycle(this);
                }
                return;
            }
        }
    }

    /**
     * 创建新订单命令。
     *
     * @param sequence 单调递增序列号
     * @param orderId 订单编号
     * @param userId 用户编号
     * @param symbolId 撮合分片编号
     * @param side 买卖方向
     * @param orderType 订单类型
     * @param tif 有效期策略
     * @param price 定点数价格
     * @param quantity 定点数数量
     * @return 新订单命令
     */
    public static Command newOrder(long sequence, long orderId, long userId, int symbolId, Side side,
                                   OrderType orderType, TimeInForce tif, long price, long quantity) {
        return newOrder(sequence, orderId, userId, symbolId, side, orderType, tif, price, quantity, 0L);
    }

    public static Command newOrder(long sequence, long orderId, long userId, int symbolId, Side side,
                                   OrderType orderType, TimeInForce tif, long price, long quantity, long expireAtEpochMillis) {
        return standalone().prepareNewOrder(sequence, orderId, userId, symbolId, side, orderType, tif, price, quantity, expireAtEpochMillis);
    }

    /**
     * 创建撤单命令。
     *
     * @param sequence 单调递增序列号
     * @param orderId 需要撤销的订单编号
     * @param symbolId 撮合分片编号
     * @return 撤单命令
     */
    public static Command cancel(long sequence, long orderId, int symbolId) {
        return standalone().prepareCancel(sequence, orderId, symbolId);
    }

    /**
     * 创建快照同步标记命令。
     *
     * @param sequence 单调递增序列号
     * @param symbolId 撮合分片编号
     * @return 快照同步标记命令
     */
    public static Command snapshotMarker(long sequence, int symbolId) {
        return standalone().prepareSnapshotMarker(sequence, symbolId);
    }

    /**
     * 创建停机命令。
     *
     * @param sequence 单调递增序列号
     * @return 停机命令
     */
    public static Command shutdown(long sequence) {
        return standalone().prepareShutdown(sequence);
    }

    public static Command pooled(Recycler recycler) {
        Command command = new Command();
        command.recycler = recycler == null ? NOOP_RECYCLER : recycler;
        command.references = command.recycler == NOOP_RECYCLER ? 0 : 1;
        return command;
    }

    private static Command standalone() {
        Command command = new Command();
        command.recycler = NOOP_RECYCLER;
        command.references = 0;
        return command;
    }

    private Command reset(CommandType type, long sequence, long orderId, long userId, int symbolId,
                          byte side, byte orderType, byte timeInForce, long price, long quantity, long expireAtEpochMillis) {
        this.type = type;
        this.sequence = sequence;
        this.orderId = orderId;
        this.userId = userId;
        this.symbolId = symbolId;
        this.side = side;
        this.orderType = orderType;
        this.timeInForce = timeInForce;
        this.price = price;
        this.quantity = quantity;
        this.expireAtEpochMillis = expireAtEpochMillis;
        if (recycler != NOOP_RECYCLER) {
            references = 1;
        }
        return this;
    }

    private void clear() {
        type = null;
        sequence = 0L;
        orderId = 0L;
        userId = 0L;
        symbolId = 0;
        side = 0;
        orderType = 0;
        timeInForce = 0;
        price = 0L;
        quantity = 0L;
        expireAtEpochMillis = 0L;
    }

    @FunctionalInterface
    public interface Recycler {
        void recycle(Command command);
    }
}
