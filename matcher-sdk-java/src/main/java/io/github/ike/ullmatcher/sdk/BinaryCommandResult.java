package io.github.ike.ullmatcher.sdk;

public record BinaryCommandResult(long orderId, long sequence, int resultCode, int reserved) {
}
