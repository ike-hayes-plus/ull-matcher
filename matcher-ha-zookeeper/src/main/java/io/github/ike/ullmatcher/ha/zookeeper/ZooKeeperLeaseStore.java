package io.github.ike.ullmatcher.ha.zookeeper;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 基于 ZooKeeper ephemeral znode 的生产向租约存储。
 * <p>
 * 独占性由 ZooKeeper session 保证；节点数据仅用于携带 owner、fencing token 和观测时间戳。
 * 这意味着真正的过期判定以 session 消失为准，而不是依赖各节点本地时钟。
 */
public final class ZooKeeperLeaseStore implements LeaseStore, Closeable {
    private static final String FIELD_SEPARATOR = "|";

    private final CuratorFramework client;
    private final String leasePath;
    private final boolean ownsClient;

    public ZooKeeperLeaseStore(ZooKeeperLeaseStoreConfig config) {
        this(
                CuratorFrameworkFactory.builder()
                        .connectString(config.connectString())
                        .sessionTimeoutMs(config.sessionTimeoutMillis())
                        .connectionTimeoutMs(config.connectionTimeoutMillis())
                        .retryPolicy(new ExponentialBackoffRetry(200, 5))
                        .build(),
                config.leasePath(),
                true
        );
    }

    public ZooKeeperLeaseStore(CuratorFramework client, String leasePath) {
        this(client, leasePath, false);
    }

    private ZooKeeperLeaseStore(CuratorFramework client, String leasePath, boolean ownsClient) {
        this.client = Objects.requireNonNull(client, "client");
        this.leasePath = Objects.requireNonNull(leasePath, "leasePath");
        this.ownsClient = ownsClient;
        if (ownsClient) {
            this.client.start();
        }
    }

    @Override
    public ClusterLease currentLease() {
        try {
            Stat stat = client.checkExists().forPath(leasePath);
            if (stat == null) {
                return null;
            }
            byte[] bytes = client.getData().storingStatIn(stat).forPath(leasePath);
            LeasePayload payload = LeasePayload.decode(bytes);
            return payload.toLease(stat.getMtime());
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException("failed to read lease from ZooKeeper path " + leasePath, e);
        }
    }

    @Override
    public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(fencingToken, "fencingToken");
        validateTiming(nowNanos, ttlNanos);
        byte[] payload = LeasePayload.encode(nodeId, fencingToken, nowNanos, ttlNanos);
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(leasePath, payload);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            try {
                return tryReacquireOwnedLease(nodeId, fencingToken, payload);
            } catch (Exception retryError) {
                throw new IllegalStateException("failed to reacquire owned lease at ZooKeeper path " + leasePath, retryError);
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed to acquire lease at ZooKeeper path " + leasePath, e);
        }
    }

    @Override
    public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(fencingToken, "fencingToken");
        validateTiming(nowNanos, ttlNanos);
        byte[] payload = LeasePayload.encode(nodeId, fencingToken, nowNanos, ttlNanos);
        try {
            Stat stat = new Stat();
            byte[] existing = client.getData().storingStatIn(stat).forPath(leasePath);
            LeasePayload parsed = LeasePayload.decode(existing);
            if (!parsed.ownerNodeId().equals(nodeId) || parsed.fencingTokenEpoch() != fencingToken.epoch()) {
                return false;
            }
            client.setData().withVersion(stat.getVersion()).forPath(leasePath, payload);
            return true;
        } catch (KeeperException.NoNodeException e) {
            return false;
        } catch (KeeperException.BadVersionException e) {
            return false;
        } catch (Exception e) {
            throw new IllegalStateException("failed to extend lease at ZooKeeper path " + leasePath, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (ownsClient) {
            client.close();
        }
    }

    private boolean tryReacquireOwnedLease(String nodeId, FencingToken fencingToken, byte[] payload) throws Exception {
        Stat stat = new Stat();
        byte[] existing = client.getData().storingStatIn(stat).forPath(leasePath);
        LeasePayload parsed = LeasePayload.decode(existing);
        if (!parsed.ownerNodeId().equals(nodeId) || parsed.fencingTokenEpoch() != fencingToken.epoch()) {
            return false;
        }
        client.setData().withVersion(stat.getVersion()).forPath(leasePath, payload);
        return true;
    }

    private static void validateTiming(long nowNanos, long ttlNanos) {
        if (nowNanos < 0L) {
            throw new IllegalArgumentException("nowNanos must be non-negative");
        }
        if (ttlNanos <= 0L) {
            throw new IllegalArgumentException("ttlNanos must be positive");
        }
    }

    private record LeasePayload(String ownerNodeId, long fencingTokenEpoch, long observedAtNanos, long requestedTtlNanos) {
        private static byte[] encode(String ownerNodeId, FencingToken fencingToken, long observedAtNanos, long requestedTtlNanos) {
            String payload = ownerNodeId + FIELD_SEPARATOR + fencingToken.epoch() + FIELD_SEPARATOR
                    + observedAtNanos + FIELD_SEPARATOR + requestedTtlNanos;
            return payload.getBytes(StandardCharsets.UTF_8);
        }

        private static LeasePayload decode(byte[] bytes) {
            String payload = new String(bytes, StandardCharsets.UTF_8);
            String[] parts = payload.split("\\|", -1);
            if (parts.length != 4) {
                throw new IllegalStateException("invalid ZooKeeper lease payload format");
            }
            return new LeasePayload(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]));
        }

        private ClusterLease toLease(long observedMillis) {
            /*
             * ZooKeeper 租约以 ephemeral znode 是否仍然存在为准。
             * 不能把一个 JVM 的 nanoTime 原样持久化后再由另一个 JVM 拿来做跨进程比较；
             * 不同 JVM 的 nanoTime 原点不保证一致。
             *
             * 对 ZooKeeper 实现而言，只要 znode 还存在，就视为租约仍然有效，
             * 真正的失效由 session 断开后节点删除来表达。
             */
            return new ClusterLease(ownerNodeId, new FencingToken(fencingTokenEpoch), Long.MAX_VALUE);
        }
    }
}
