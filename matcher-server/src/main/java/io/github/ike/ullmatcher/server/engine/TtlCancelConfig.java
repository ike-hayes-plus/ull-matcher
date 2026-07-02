package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.TimeInForce;

import java.util.concurrent.TimeUnit;

public record TtlCancelConfig(
        boolean enabled,
        long sweepIntervalMillis,
        long defaultTtlMillis,
        long hardTtlMillis,
        long recoveredOrderTtlMillis,
        int recentAuditLimit
) {
    public TtlCancelConfig {
        if (sweepIntervalMillis < 0L || defaultTtlMillis < 0L || hardTtlMillis < 0L
                || recoveredOrderTtlMillis < 0L || recentAuditLimit < 0) {
            throw new IllegalArgumentException("ttl cancel configuration must be non-negative");
        }
        if (enabled && sweepIntervalMillis <= 0L) {
            throw new IllegalArgumentException("ttl sweepIntervalMillis must be positive when enabled");
        }
    }

    public static TtlCancelConfig disabled() {
        return new TtlCancelConfig(false, 0L, 0L, 0L, 0L, 0);
    }

    public static TtlCancelConfig defaults() {
        return new TtlCancelConfig(false, 1_000L, 0L, TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(7), 128);
    }

    public long resolveExpireAtEpochMillis(TimeInForce timeInForce, Long requestedTtlMillis, long acceptedAtEpochMillis) {
        if (!enabled || acceptedAtEpochMillis <= 0L) {
            return 0L;
        }
        if (timeInForce == TimeInForce.IOC || timeInForce == TimeInForce.FOK) {
            return 0L;
        }
        long ttl = clampTtl(requestedTtlMillis, defaultTtlMillis, hardTtlMillis);
        return ttl <= 0L ? 0L : acceptedAtEpochMillis + ttl;
    }

    public long resolveRecoveredExpireAtEpochMillis(long persistedExpireAtEpochMillis, long nowEpochMillis) {
        if (!enabled) {
            return 0L;
        }
        if (persistedExpireAtEpochMillis > 0L) {
            return persistedExpireAtEpochMillis;
        }
        if (recoveredOrderTtlMillis > 0L) {
            return nowEpochMillis + recoveredOrderTtlMillis;
        }
        if (hardTtlMillis > 0L) {
            return nowEpochMillis + hardTtlMillis;
        }
        return 0L;
    }

    private static long clampTtl(Long requestedTtlMillis, long defaultTtlMillis, long hardTtlMillis) {
        long ttl = requestedTtlMillis != null && requestedTtlMillis > 0L ? requestedTtlMillis : defaultTtlMillis;
        if (ttl <= 0L && hardTtlMillis > 0L) {
            ttl = hardTtlMillis;
        }
        if (hardTtlMillis > 0L && ttl > hardTtlMillis) {
            ttl = hardTtlMillis;
        }
        return Math.max(ttl, 0L);
    }
}
