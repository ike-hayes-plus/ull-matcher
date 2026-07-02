package io.github.ike.ullmatcher.core;

/** 单线程对象池，用于移除热路径分配压力。 */
public final class OrderPool {
    /** 可复用订单节点的后备栈。 */
    private final Order[] pool;

    /** 当前栈顶后一位索引。 */
    private int top;

    /** 对象池耗尽次数。 */
    private long exhaustedBorrows;

    /**
     * 创建预填充订单节点的对象池。
     *
     * @param capacity 预分配节点数量
     */
    public OrderPool(int capacity) {
        pool = new Order[capacity];
        for (int i = 0; i < capacity; i++) pool[i] = new Order();
        top = capacity;
    }

    /**
     * 借出订单节点。
     *
     * @return 池化节点；对象池耗尽时返回 {@code null}
     */
    public Order borrow() {
        if (top == 0) {
            exhaustedBorrows++;
            return null;
        }
        return pool[--top];
    }

    /**
     * 重置节点，并在对象池有容量时归还。
     *
     * @param order 待回收订单节点
     */
    public void release(Order order) {
        order.reset();
        if (top < pool.length) pool[top++] = order;
    }

    /**
     * 返回当前可借出的订单节点数量。
     *
     * @return 可用节点数量
     */
    public int available() {
        return top;
    }

    /**
     * 返回对象池耗尽次数。
     *
     * @return 耗尽次数
     */
    public long exhaustedBorrows() {
        return exhaustedBorrows;
    }
}
