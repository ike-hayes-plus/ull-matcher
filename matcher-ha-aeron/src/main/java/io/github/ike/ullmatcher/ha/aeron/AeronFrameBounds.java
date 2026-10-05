package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.DirectBuffer;

/**
 * 校验解码窗口，避免按线路上的长度字段分配数组。
 */
final class AeronFrameBounds {
    private AeronFrameBounds() {
    }

    static int end(DirectBuffer buffer, int offset, int length) {
        if (offset < 0 || length < 0 || length > buffer.capacity() || offset > buffer.capacity() - length) {
            throw new IllegalArgumentException("frame is outside the buffer");
        }
        return offset + length;
    }

    static void requireHeader(int offset, int end, int headerLength) {
        if (end - offset < headerLength) {
            throw new IllegalArgumentException("frame is truncated");
        }
    }

    static int fitting(int declared, int start, int end) {
        return fitting(declared, start, end, Integer.MAX_VALUE);
    }

    static int fitting(int declared, int start, int end, int maxInclusive) {
        if (start < 0 || end < start || declared < 0 || declared > maxInclusive || declared > end - start) {
            throw new IllegalArgumentException("field length exceeds frame");
        }
        return declared;
    }

    static void fittingCount(int count, int elementBytes, int start, int end) {
        if (count < 0 || elementBytes <= 0 || start < 0 || end < start || count > (end - start) / elementBytes) {
            throw new IllegalArgumentException("field count exceeds frame");
        }
    }
}
