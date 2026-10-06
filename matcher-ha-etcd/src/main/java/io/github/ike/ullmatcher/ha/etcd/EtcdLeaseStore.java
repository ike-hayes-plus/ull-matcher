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

    public EtcdLeaseStore(EtcdConfig config) throws IOException {
        this(new EtcdClient(config), normalizePrefix(config.keyPrefix()) + "/lease/primary");
    }

    EtcdLeaseStore(EtcdClient client, String leaseKey) {
        this.client = Objects.requireNonNull(client, "client");
        this.leaseKey = Objects.requireNonNull(leaseKey, "leaseKey");
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
            long leaseId = client.grantLease(grantTtlSeconds(ttlNanos));
            String payload = LeasePayload.encode(nodeId, fencingToken);
            return client.txnCreate(leaseKey, payload, leaseId);
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
            long leaseId = client.grantLease(grantTtlSeconds(ttlNanos));
            String payload = LeasePayload.encode(nodeId, fencingToken);
            return client.txnReplaceIfValue(leaseKey, payload, payload, leaseId);
        } catch (IOException e) {
            throw new IllegalStateException("failed to extend lease at etcd key " + leaseKey, e);
        }
    }

    @Override
    public boolean isHeldBy(String nodeId, FencingToken fencingToken, long nowNanos) {
        ClusterLease lease = currentLease();
        return lease != null
                && !lease.isExpired(nowNanos)
                && lease.ownerNodeId().equals(nodeId)
                && lease.fencingToken().equals(fencingToken);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    static long grantTtlSeconds(long ttlNanos) {
        if (ttlNanos <= 0L) {
            throw new IllegalArgumentException("ttlNanos must be positive");
        }
        return Math.max(1L, (ttlNanos + 1_000_000_000L - 1L) / 1_000_000_000L);
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
