package io.github.ike.ullmatcher.util;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * 热路径使用的极简开放寻址 long 到对象映射。
 * <p>
 * 这不是通用集合：键 0 被保留为空槽标记。
 * @param <V> 值类型
 */
public final class LongObjectHashMap<V> {
    /** 扩容前允许的最大负载因子。 */
    private static final float LOAD = 0.60f;

    /** 键表；{@code 0} 表示空槽。 */
    private long[] keys;

    /** 与 {@link #keys} 对齐的值表。 */
    private Object[] values;

    /** 用于快速取模的容量掩码。 */
    private int mask;

    /** 活跃条目数量。 */
    private int size;

    /** 扩容阈值。 */
    private int resizeAt;

    /**
     * 按预期条目数量创建映射。
     *
     * @param expected 预期条目数量
     */
    public LongObjectHashMap(int expected) {
        int cap = 1;
        while (cap < expected * 2) cap <<= 1;
        keys = new long[cap]; values = new Object[cap]; mask = cap - 1; resizeAt = (int)(cap * LOAD);
    }

    /**
     * 按键查找值。
     *
     * @param key 非零键
     * @return 已存储值；不存在时返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public V get(long key) {
        if (key == 0) throw new IllegalArgumentException("key 0 is reserved");
        int i = mix(key) & mask;
        while (true) {
            long k = keys[i];
            if (k == 0) return null;
            if (k == key) return (V) values[i];
            i = (i + 1) & mask;
        }
    }

    /**
     * 插入或替换一个值。
     *
     * @param key 非零键
     * @param value 待存储值
     * @return 旧值；键不存在时返回 {@code null}
     */
    public V put(long key, V value) {
        if (key == 0) throw new IllegalArgumentException("key 0 is reserved");
        if (size >= resizeAt) rehash(keys.length << 1);
        return putNoResize(key, value);
    }

    /**
     * 不检查负载因子地插入或替换。
     *
     * @param key 非零键
     * @param value 待存储值
     * @return 旧值；键不存在时返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    private V putNoResize(long key, V value) {
        int i = mix(key) & mask;
        while (true) {
            long k = keys[i];
            if (k == 0) { keys[i] = key; values[i] = value; size++; return null; }
            if (k == key) { V old = (V) values[i]; values[i] = value; return old; }
            i = (i + 1) & mask;
        }
    }

    /**
     * 移除一个键。
     *
     * @param key 非零键
     * @return 被移除值；不存在时返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public V remove(long key) {
        if (key == 0) throw new IllegalArgumentException("key 0 is reserved");
        int i = mix(key) & mask;
        while (true) {
            long k = keys[i];
            if (k == 0) return null;
            if (k == key) {
                V old = (V) values[i];
                keys[i] = 0; values[i] = null; size--;
                compactCluster(i);
                return old;
            }
            i = (i + 1) & mask;
        }
    }

    /**
     * 判断键是否存在。
     *
     * @param key 非零键
     * @return 存在时返回 {@code true}
     */
    public boolean containsKey(long key) {
        return get(key) != null;
    }

    /**
     * 判断在不触发表扩容的前提下是否可以写入指定键。
     *
     * @param key 非零键
     * @return 键已存在或仍有扩容阈值内容量时返回 {@code true}
     */
    public boolean canInsertWithoutResize(long key) {
        if (key == 0) throw new IllegalArgumentException("key 0 is reserved");
        return containsKey(key) || size < resizeAt;
    }

    /**
     * 返回条目数量。
     *
     * @return 条目数量
     */
    public int size() {
        return size;
    }

    /**
     * 无分配遍历活跃值。
     * <p>
     * 只能由拥有线程调用。
     *
     * @param consumer 值消费者
     */
    @SuppressWarnings("unchecked")
    public void forEachValue(Consumer<V> consumer) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != 0) consumer.accept((V) values[i]);
        }
    }

    /**
     * 重新插入删除槽之后的条目，保证探测链仍然可达。
     *
     * @param deleteIndex 被删除的槽位索引
     */
    @SuppressWarnings("unchecked")
    private void compactCluster(int deleteIndex) {
        int i = (deleteIndex + 1) & mask;
        while (keys[i] != 0) {
            long k = keys[i]; Object v = values[i];
            keys[i] = 0; values[i] = null; size--;
            putNoResize(k, (V) v);
            i = (i + 1) & mask;
        }
    }

    /**
     * 以新容量重建表。
     *
     * @param newCap 新的 2 的幂容量
     */
    @SuppressWarnings("unchecked")
    private void rehash(int newCap) {
        long[] oldK = keys; Object[] oldV = values;
        keys = new long[newCap]; values = new Object[newCap]; mask = newCap - 1; resizeAt = (int)(newCap * LOAD); size = 0;
        for (int i = 0; i < oldK.length; i++) if (oldK[i] != 0) putNoResize(oldK[i], (V) oldV[i]);
        Arrays.fill(oldV, null);
    }

    /**
     * 将 long 键混合成 32 位哈希。
     *
     * @param x 待混合键
     * @return 混合后的哈希
     */
    private static int mix(long x) {
        x ^= x >>> 33; x *= 0xff51afd7ed558ccdL; x ^= x >>> 33; x *= 0xc4ceb9fe1a85ec53L; x ^= x >>> 33;
        return (int)x;
    }
}
