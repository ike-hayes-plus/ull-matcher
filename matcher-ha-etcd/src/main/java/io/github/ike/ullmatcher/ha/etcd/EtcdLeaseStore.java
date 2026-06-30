package io.github.ike.ullmatcher.ha.etcd;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * etcd-backed primary lease store.
 * <p>
 * The lease key is attached to an etcd lease. The key disappearing means the
 * owner is no longer fenced as primary. Fencing identity is carried in the value.
 */
public final class EtcdLeaseStore implements LeaseStore, Closeable {
    private static final String FIELD_SEPARATOR = "|";

    private final EtcdClient client;
    private final String leaseKey;
    private final long leaseTtlSeconds;
    private final long localHeldCheckCacheNanos;
    private volatile CachedHeldLease cachedHeldLease;

    public EtcdLeaseStore(EtcdConfig config) {
        this(new EtcdClient(config.endpoint(), config.timeoutMillis()),
                normalizePrefix(config.keyPrefix()) + "/lease/primary",
                config.leaseTtlSeconds(),
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.localHeldCheckCacheMillis()));
    }

    EtcdLeaseStore(EtcdClient client, String leaseKey, long leaseTtlSeconds, long localHeldCheckCacheNanos) {
        this.client = Objects.requireNonNull(client, "client");
        this.leaseKey = Objects.requireNonNull(leaseKey, "leaseKey");
        if (leaseTtlSeconds <= 0L) {
            throw new IllegalArgumentException("leaseTtlSeconds must be positive");
        }
        if (localHeldCheckCacheNanos < 0L) {
            throw new IllegalArgumentException("localHeldCheckCacheNanos must be non-negative");
        }
        this.leaseTtlSeconds = leaseTtlSeconds;
        this.localHeldCheckCacheNanos = localHeldCheckCacheNanos;
    }

    @Override
    public ClusterLease currentLease() {
        try {
            EtcdClient.KeyValue keyValue = client.get(leaseKey);
            if (keyValue == null) {
                return null;
            }
            return LeasePayload.decode(keyValue.value()).toLease();
        } catch (IOException e) {
            throw new IllegalStateException("failed to read lease from etcd key " + leaseKey, e);
        }
    }

    @Override
    public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(fencingToken, "fencingToken");
        validateTiming(nowNanos, ttlNanos);
        try {
            long leaseId = client.grantLease(leaseTtlSeconds);
            String payload = LeasePayload.encode(nodeId, fencingToken);
            boolean acquired = client.txnCreate(leaseKey, payload, leaseId);
            if (acquired) {
                cacheHeld(nodeId, fencingToken, nowNanos);
            }
            return acquired;
        } catch (IOException e) {
            throw new IllegalStateException("failed to acquire lease at etcd key " + leaseKey, e);
        }
    }

    @Override
    public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(fencingToken, "fencingToken");
        validateTiming(nowNanos, ttlNanos);
        try {
            long leaseId = client.grantLease(leaseTtlSeconds);
            String payload = LeasePayload.encode(nodeId, fencingToken);
            boolean extended = client.txnReplaceIfValue(leaseKey, payload, payload, leaseId);
            if (extended) {
                cacheHeld(nodeId, fencingToken, nowNanos);
            } else {
                cachedHeldLease = null;
            }
            return extended;
        } catch (IOException e) {
            throw new IllegalStateException("failed to extend lease at etcd key " + leaseKey, e);
        }
    }

    @Override
    public boolean isHeldBy(String nodeId, FencingToken fencingToken, long nowNanos) {
        CachedHeldLease cached = cachedHeldLease;
        if (cached != null
                && cached.matches(nodeId, fencingToken)
                && nowNanos < cached.expiresAtNanos()) {
            return true;
        }
        ClusterLease lease = currentLease();
        boolean held = lease != null
                && !lease.isExpired(nowNanos)
                && lease.ownerNodeId().equals(nodeId)
                && lease.fencingToken().equals(fencingToken);
        if (held) {
            cacheHeld(nodeId, fencingToken, nowNanos);
        } else {
            cachedHeldLease = null;
        }
        return held;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static void validateTiming(long nowNanos, long ttlNanos) {
        if (nowNanos < 0L) {
            throw new IllegalArgumentException("nowNanos must be non-negative");
        }
        if (ttlNanos <= 0L) {
            throw new IllegalArgumentException("ttlNanos must be positive");
        }
    }

    private static String normalizePrefix(String prefix) {
        return prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
    }

    private void cacheHeld(String nodeId, FencingToken fencingToken, long nowNanos) {
        if (localHeldCheckCacheNanos == 0L) {
            return;
        }
        cachedHeldLease = new CachedHeldLease(nodeId, fencingToken, nowNanos + localHeldCheckCacheNanos);
    }

    private record CachedHeldLease(String nodeId, FencingToken fencingToken, long expiresAtNanos) {
        private boolean matches(String nodeId, FencingToken fencingToken) {
            return this.nodeId.equals(nodeId) && this.fencingToken.equals(fencingToken);
        }
    }

    private record LeasePayload(String ownerNodeId, long fencingTokenEpoch) {
        private static String encode(String ownerNodeId, FencingToken fencingToken) {
            return ownerNodeId + FIELD_SEPARATOR + fencingToken.epoch();
        }

        private static LeasePayload decode(String payload) {
            String[] parts = payload.split("\\|", -1);
            if (parts.length != 2) {
                throw new IllegalStateException("invalid etcd lease payload format");
            }
            return new LeasePayload(parts[0], Long.parseLong(parts[1]));
        }

        private ClusterLease toLease() {
            return new ClusterLease(ownerNodeId, new FencingToken(fencingTokenEpoch), Long.MAX_VALUE);
        }
    }
}
