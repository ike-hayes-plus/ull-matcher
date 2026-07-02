package io.github.ike.ullmatcher.ha.discovery;

import io.github.ike.ullmatcher.ha.coordination.HaRole;

import java.util.Map;
import java.util.Objects;

/**
 * 发现到的复制节点信息。
 *
 * @param nodeId 节点标识
 * @param host gRPC 复制平面地址
 * @param grpcPort gRPC 复制平面端口
 * @param role 节点当前角色
 * @param metadata 节点元数据
 */
public record DiscoveredNode(
        String nodeId,
        String host,
        int grpcPort,
        HaRole role,
        Map<String, String> metadata
) {
    public DiscoveredNode {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(role, "role");
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        if (nodeId.isBlank() || host.isBlank()) {
            throw new IllegalArgumentException("nodeId and host must not be blank");
        }
        if (grpcPort <= 0 || grpcPort > 65_535) {
            throw new IllegalArgumentException("grpcPort must be between 1 and 65535");
        }
    }

    public String endpoint() {
        return host + ":" + grpcPort;
    }
}
