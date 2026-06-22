package io.github.ike.ullmatcher.discovery.zookeeper;

import java.util.Objects;

public record ZooKeeperDiscoveryConfig(
        String connectString,
        String servicePath,
        int sessionTimeoutMillis,
        int connectionTimeoutMillis
) {
    public ZooKeeperDiscoveryConfig {
        Objects.requireNonNull(connectString, "connectString");
        Objects.requireNonNull(servicePath, "servicePath");
        if (connectString.isBlank() || servicePath.isBlank()) {
            throw new IllegalArgumentException("connectString and servicePath must not be blank");
        }
        if (sessionTimeoutMillis <= 0 || connectionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("ZooKeeper timeouts must be positive");
        }
    }

    public static ZooKeeperDiscoveryConfig defaults(String connectString, String clusterName) {
        return new ZooKeeperDiscoveryConfig(connectString, "/ull-matcher/discovery/" + clusterName + "/nodes", 15_000, 5_000);
    }
}
