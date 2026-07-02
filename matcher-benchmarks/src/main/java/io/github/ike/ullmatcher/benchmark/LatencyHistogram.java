package io.github.ike.ullmatcher.benchmark;

import java.util.Arrays;

/**
 * 仅供压测和本地分析使用的有界延迟直方图。
 */
public final class LatencyHistogram {
    private final long[] samples;
    private int size;

    public LatencyHistogram(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.samples = new long[capacity];
    }

    public void record(long nanos) {
        if (size < samples.length) {
            samples[size++] = nanos;
        }
    }

    public long percentile(double percentile) {
        if (percentile < 0.0 || percentile > 100.0) {
            throw new IllegalArgumentException("percentile must be in [0, 100]");
        }
        if (size == 0) {
            return 0L;
        }
        long[] copy = Arrays.copyOf(samples, size);
        Arrays.sort(copy);
        int index = Math.min(copy.length - 1, (int) Math.ceil(percentile / 100.0 * copy.length) - 1);
        return copy[index];
    }

    public int size() {
        return size;
    }
}
