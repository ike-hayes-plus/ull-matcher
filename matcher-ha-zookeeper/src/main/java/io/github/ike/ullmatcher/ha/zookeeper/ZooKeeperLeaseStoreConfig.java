package io.github.ike.ullmatcher.ha.zookeeper;

import java.util.Objects;

/**
 * ZooKeeper LeaseStore 配置。
 *
 * @param connectString ZooKeeper 连接串
 * @param leasePath 单个分片的租约节点路径
 * @param sessionTimeoutMillis 会话超时
 * @param connectionTimeoutMillis 连接超时
 */
public record ZooKeeperLeaseStoreConfig(
        String connectString,
        String leasePath,
        int sessionTimeoutMillis,
        int connectionTimeoutMillis
) {
    public ZooKeeperLeaseStoreConfig {
        Objects.requireNonNull(connectString, "connectString");
        Objects.requireNonNull(leasePath, "leasePath");
        if (connectString.isBlank()) {
            throw new IllegalArgumentException("connectString must not be blank");
        }
        if (leasePath.isBlank() || !leasePath.startsWith("/")) {
            throw new IllegalArgumentException("leasePath must be an absolute ZooKeeper path");
        }
        if (sessionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("sessionTimeoutMillis must be positive");
        }
        if (connectionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectionTimeoutMillis must be positive");
        }
    }

    public static ZooKeeperLeaseStoreConfig of(String connectString, String leasePath) {
        return new ZooKeeperLeaseStoreConfig(connectString, leasePath, 15_000, 5_000);
    }
}
