package io.github.ike.ullmatcher.util;

import java.util.Arrays;

/** 用于价格发现的基本类型二叉堆。懒删除由调用方处理。 */
public final class LongHeap {
    /** 堆数组。 */
    private long[] a;

    /** 堆内元素数量。 */
    private int size;

    /** {@code true} 表示最大堆，{@code false} 表示最小堆。 */
    private final boolean max;

    /**
     * 创建基本类型堆。
     *
     * @param capacity 请求的初始容量
     * @param max {@code true} 表示最大堆，{@code false} 表示最小堆
     */
    public LongHeap(int capacity, boolean max) {
        a = new long[Math.max(16, capacity)]; this.max = max;
    }

    /**
     * 向堆中加入一个值。
     *
     * @param v 待加入值
     */
    public void add(long v) {
        if (size == a.length) grow();
        addUnchecked(v);
    }

    /**
     * 在调用方已经确认容量足够时加入一个值。
     *
     * @param v 待加入值
     */
    private void addUnchecked(long v) {
        int i = size++; a[i] = v;
        while (i > 0) {
            int p = (i - 1) >>> 1;
            if (!better(a[i], a[p])) break;
            swap(i, p); i = p;
        }
    }

    /**
     * 返回堆顶值但不移除。
     *
     * @return 堆顶值；为空时返回 {@code 0}
     */
    public long peek() {
        return size == 0 ? 0 : a[0];
    }

    /**
     * 移除并返回堆顶值。
     *
     * @return 堆顶值；为空时返回 {@code 0}
     */
    public long poll() {
        if (size == 0) return 0;
        long r = a[0]; a[0] = a[--size]; siftDown(0); return r;
    }

    /**
     * 判断堆是否为空。
     *
     * @return 未存储任何值时返回 {@code true}
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * 清空堆内容但保留底层数组。
     */
    public void clear() {
        size = 0;
    }

    /**
     * 创建当前堆内容的浅拷贝，用于不修改原堆的顺序扫描。
     *
     * @return 堆副本
     */
    public LongHeap copy() {
        LongHeap copy = new LongHeap(16, max);
        copy.a = Arrays.copyOf(a, a.length);
        copy.size = size;
        return copy;
    }

    /**
     * 将当前堆内容复制到目标堆，用于复用扫描缓冲区。
     *
     * @param target 目标堆，方向必须与当前堆一致
     */
    public void copyInto(LongHeap target) {
        if (target.max != max) {
            throw new IllegalArgumentException("heap direction mismatch");
        }
        if (target.a.length < size) {
            target.a = Arrays.copyOf(target.a, a.length);
        }
        System.arraycopy(a, 0, target.a, 0, size);
        target.size = size;
    }

    /**
     * 判断堆是否仍有空闲槽位。
     *
     * @return 未达到容量上限时返回 {@code true}
     */
    public boolean hasCapacity() {
        return size < a.length;
    }

    /**
     * 恢复指定索引以下的堆不变式。
     *
     * @param i 需要下沉的索引
     */
    private void siftDown(int i) {
        while (true) {
            int l = i * 2 + 1, r = l + 1, b = i;
            if (l < size && better(a[l], a[b])) b = l;
            if (r < size && better(a[r], a[b])) b = r;
            if (b == i) return;
            swap(i, b); i = b;
        }
    }

    /**
     * 按堆方向比较两个值。
     *
     * @param x 候选值
     * @param y 当前值
     * @return {@code x} 应位于 {@code y} 上方时返回 {@code true}
     */
    private boolean better(long x, long y) {
        return max ? x > y : x < y;
    }

    /**
     * 交换两个堆槽位。
     *
     * @param i 第一个索引
     * @param j 第二个索引
     */
    private void swap(int i, int j) {
        long t = a[i]; a[i] = a[j]; a[j] = t;
    }

    /**
     * 将堆容量扩大一倍。
     */
    private void grow() {
        long[] n = new long[a.length << 1]; System.arraycopy(a, 0, n, 0, a.length); a = n;
    }
}
