package io.github.ike.ullmatcher.ha.etcd;

import java.util.Objects;

/**
 * etcd control-plane configuration.
 *
 * @param endpoint etcd HTTP endpoint, for example {@code http://127.0.0.1:2379}
 * @param keyPrefix key prefix used by this matcher deployment
 * @param leaseTtlSeconds etcd lease TTL in seconds
 * @param timeoutMillis HTTP request timeout in milliseconds
 * @param localHeldCheckCacheMillis positive local cache duration for hot-path ownership checks
 */
public record EtcdConfig(
        String endpoint,
        String keyPrefix,
        long leaseTtlSeconds,
        long timeoutMillis,
        long localHeldCheckCacheMillis
) {
    public EtcdConfig {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        if (endpoint.isBlank() || keyPrefix.isBlank() || !keyPrefix.startsWith("/")) {
            throw new IllegalArgumentException("endpoint must not be blank and keyPrefix must be an absolute path");
        }
        if (leaseTtlSeconds <= 0L || timeoutMillis <= 0L || localHeldCheckCacheMillis < 0L) {
            throw new IllegalArgumentException("leaseTtlSeconds and timeoutMillis must be positive; localHeldCheckCacheMillis must be non-negative");
        }
    }

    public static EtcdConfig defaults(String endpoint, String clusterName) {
        return new EtcdConfig(endpoint, "/ull-matcher/" + clusterName, 10L, 2_000L, 25L);
    }
}
