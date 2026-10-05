package io.github.ike.ullmatcher.orchestrator;

import java.util.Objects;

/**
 * 分片对外服务端点（与 {@code matcher-server} 绑定地址一致）。
 */
public record ShardEndpoints(
        String httpHost,
        int httpPort,
        int grpcPort,
        int binaryIngressPort
) {
    public ShardEndpoints {
        Objects.requireNonNull(httpHost, "httpHost");
        if (httpHost.isBlank()) {
            throw new IllegalArgumentException("httpHost must not be blank");
        }
        if (httpPort <= 0 || grpcPort <= 0 || binaryIngressPort < 0) {
            throw new IllegalArgumentException("invalid shard endpoint ports");
        }
    }
}
