package io.github.ike.ullmatcher.discovery.nacos;

import java.util.Objects;

public record NacosDiscoveryConfig(
        String serverAddress,
        String serviceName,
        String groupName,
        String namespace,
        String clusterName
) {
    public NacosDiscoveryConfig {
        Objects.requireNonNull(serverAddress, "serverAddress");
        Objects.requireNonNull(serviceName, "serviceName");
        Objects.requireNonNull(groupName, "groupName");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(clusterName, "clusterName");
        if (serverAddress.isBlank() || serviceName.isBlank() || groupName.isBlank()) {
            throw new IllegalArgumentException("Nacos serverAddress, serviceName, and groupName must not be blank");
        }
    }

    public static NacosDiscoveryConfig defaults(String serverAddress, String clusterName) {
        return new NacosDiscoveryConfig(serverAddress, "ull-matcher-replication", "ULL_MATCHER", "", clusterName);
    }
}
