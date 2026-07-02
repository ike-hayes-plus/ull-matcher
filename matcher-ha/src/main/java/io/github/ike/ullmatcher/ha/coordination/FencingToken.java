package io.github.ike.ullmatcher.ha.coordination;

/**
 * 防双主栅栏令牌。
 *
 * @param epoch 单调递增纪元
 */
public record FencingToken(long epoch) {
    public FencingToken {
        if (epoch <= 0L) {
            throw new IllegalArgumentException("epoch must be positive");
        }
    }

    public FencingToken next() {
        return new FencingToken(epoch + 1L);
    }
}
