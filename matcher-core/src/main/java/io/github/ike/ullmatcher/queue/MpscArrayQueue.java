package io.github.ike.ullmatcher.queue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 有界多生产者单消费者数组队列。
 *
 * @param <E> 队列元素类型
 */
public final class MpscArrayQueue<E> {
    private static final VarHandle ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);

    private final Object[] buffer;
    private final int mask;
    private final AtomicLong producerSeq = new AtomicLong();

    private volatile long consumerSeq;
    private long cachedConsumerSeq;

    /**
     * 创建队列。
     *
     * @param capacityPowerOfTwo 容量，必须是 2 的幂
     */
    public MpscArrayQueue(int capacityPowerOfTwo) {
        if (Integer.bitCount(capacityPowerOfTwo) != 1) {
            throw new IllegalArgumentException("capacity must be power of two");
        }
        this.buffer = new Object[capacityPowerOfTwo];
        this.mask = capacityPowerOfTwo - 1;
    }

    /**
     * 非阻塞发布元素。
     *
     * @param element 待发布元素
     * @return 队列已满时返回 {@code false}
     */
    public boolean offer(E element) {
        Objects.requireNonNull(element, "element");
        while (true) {
            long producer = producerSeq.get();
            long wrapPoint = producer - buffer.length;
            long cachedConsumer = cachedConsumerSeq;
            if (cachedConsumer <= wrapPoint) {
                cachedConsumer = consumerSeq;
                cachedConsumerSeq = cachedConsumer;
                if (cachedConsumer <= wrapPoint) {
                    return false;
                }
            }
            if (producerSeq.compareAndSet(producer, producer + 1)) {
                ARRAY.setRelease(buffer, (int) (producer & mask), element);
                return true;
            }
        }
    }

    /**
     * 轮询一个元素。
     *
     * @return 队列为空时返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public E poll() {
        long consumer = consumerSeq;
        if (consumer >= producerSeq.get()) {
            return null;
        }
        int index = (int) (consumer & mask);
        E element = (E) ARRAY.getAcquire(buffer, index);
        while (element == null) {
            if (consumer >= producerSeq.get()) {
                return null;
            }
            Thread.onSpinWait();
            element = (E) ARRAY.getAcquire(buffer, index);
        }
        ARRAY.setRelease(buffer, index, null);
        consumerSeq = consumer + 1;
        return element;
    }

    /**
     * 批量转移到目标集合。
     *
     * @param target 目标集合
     * @param limit 最大转移数量
     * @return 实际转移数量
     */
    public int drainTo(Collection<? super E> target, int limit) {
        Objects.requireNonNull(target, "target");
        if (limit <= 0) {
            return 0;
        }
        int drained = 0;
        while (drained < limit) {
            E element = poll();
            if (element == null) {
                break;
            }
            target.add(element);
            drained++;
        }
        return drained;
    }

    /**
     * 返回近似排队元素数量。
     *
     * @return 队列元素数量
     */
    public int size() {
        return (int) (producerSeq.get() - consumerSeq);
    }

    /**
     * 返回队列容量。
     *
     * @return 队列容量
     */
    public int capacity() {
        return buffer.length;
    }
}
