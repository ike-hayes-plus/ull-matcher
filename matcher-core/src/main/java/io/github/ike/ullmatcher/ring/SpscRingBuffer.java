package io.github.ike.ullmatcher.ring;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 极简单生产者单消费者环形缓冲区。
 * <p>
 * 用于网关或定序器与一个撮合分片之间通信。容量必须是 2 的幂。
 * @param <E> 环形缓冲区承载的元素类型
 */
public final class SpscRingBuffer<E> {
    /** 用于数组释放/获取语义访问的变量句柄。 */
    private static final VarHandle ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);

    /** 环形槽位。 */
    private final Object[] buffer;

    /** 替代取模运算的位掩码；容量为 2 的幂时有效。 */
    private final int mask;

    /** 下一个生产者序列号。 */
    private volatile long producerSeq;

    /** 下一个消费者序列号。 */
    private volatile long consumerSeq;

    /**
     * 创建环形缓冲区。
     *
     * @param capacityPowerOfTwo 容量，必须是 2 的幂
     */
    public SpscRingBuffer(int capacityPowerOfTwo) {
        if (Integer.bitCount(capacityPowerOfTwo) != 1) throw new IllegalArgumentException("capacity must be power of two");
        buffer = new Object[capacityPowerOfTwo]; mask = capacityPowerOfTwo - 1;
    }

    /**
     * 非阻塞发布元素。
     *
     * @param e 待发布元素
     * @return 环形缓冲区已满时返回 {@code false}
     */
    public boolean offer(E e) {
        long p = producerSeq;
        if (p - consumerSeq >= buffer.length) return false;
        ARRAY.setRelease(buffer, (int)(p & mask), e);
        producerSeq = p + 1;
        return true;
    }

    /**
     * 以最低延迟轮询一个元素。
     * <p>
     * 调用方可围绕 {@code null} 结果增加退避策略。
     *
     * @return 下一个元素；环形缓冲区为空时返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public E poll() {
        long c = consumerSeq;
        if (c >= producerSeq) return null;
        int idx = (int)(c & mask);
        E e = (E) ARRAY.getAcquire(buffer, idx);
        ARRAY.setRelease(buffer, idx, null);
        consumerSeq = c + 1;
        return e;
    }

    /**
     * 批量轮询多个元素到调用方提供的数组。
     *
     * @param target 目标数组
     * @param limit 最多转移数量
     * @return 实际转移数量
     */
    @SuppressWarnings("unchecked")
    public int drain(E[] target, int limit) {
        if (limit <= 0) {
            return 0;
        }
        long c = consumerSeq;
        long p = producerSeq;
        int available = (int) Math.min(limit, p - c);
        if (available <= 0) {
            return 0;
        }
        int drained = 0;
        while (drained < available) {
            int idx = (int) ((c + drained) & mask);
            E e = (E) ARRAY.getAcquire(buffer, idx);
            while (e == null) {
                if (c + drained >= producerSeq) {
                    consumerSeq = c + drained;
                    return drained;
                }
                Thread.onSpinWait();
                e = (E) ARRAY.getAcquire(buffer, idx);
            }
            target[drained] = e;
            ARRAY.setRelease(buffer, idx, null);
            drained++;
        }
        consumerSeq = c + drained;
        return drained;
    }

    /**
     * 返回近似排队元素数量。
     *
     * @return 排队元素数量
     */
    public int size() {
        return (int)(producerSeq - consumerSeq);
    }

    /**
     * 返回近似剩余容量。
     *
     * @return 剩余容量
     */
    public int remainingCapacity() {
        return buffer.length - size();
    }

    /**
     * 返回环形缓冲区总容量。
     *
     * @return 总容量
     */
    public int capacity() {
        return buffer.length;
    }
}
