package io.github.ike.ullmatcher.ha.grpc.server;

import io.github.ike.ullmatcher.ha.grpc.security.GrpcServerTlsConfig;

public record GrpcReplicationServerConfig(
        String bindHost,
        int port,
        int maxInboundMessageSize,
        long permitKeepAliveTimeSeconds,
        long replicationIngressTimeoutMillis,
        String compressionCodec,
        GrpcServerTlsConfig tls
) {
    public GrpcReplicationServerConfig {
        if (bindHost == null || bindHost.isBlank()) {
            throw new IllegalArgumentException("bindHost must not be blank");
        }
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (maxInboundMessageSize <= 0) {
            throw new IllegalArgumentException("maxInboundMessageSize must be positive");
        }
        if (permitKeepAliveTimeSeconds < 0L) {
            throw new IllegalArgumentException("permitKeepAliveTimeSeconds must be non-negative");
        }
        if (replicationIngressTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("replicationIngressTimeoutMillis must be positive");
        }
        if (compressionCodec == null || compressionCodec.isBlank()) {
            throw new IllegalArgumentException("compressionCodec must not be blank");
        }
    }

    public static GrpcReplicationServerConfig defaults(int port) {
        return new GrpcReplicationServerConfig("127.0.0.1", port, 4 << 20, 30L, 2_000L, "identity", null);
    }

    public GrpcReplicationServerConfig withBindHost(String bindHost) {
        return new GrpcReplicationServerConfig(
                bindHost,
                port,
                maxInboundMessageSize,
                permitKeepAliveTimeSeconds,
                replicationIngressTimeoutMillis,
                compressionCodec,
                tls
        );
    }
}
