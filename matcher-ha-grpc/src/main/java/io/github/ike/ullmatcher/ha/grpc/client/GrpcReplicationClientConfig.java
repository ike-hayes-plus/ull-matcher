package io.github.ike.ullmatcher.ha.grpc.client;

import io.github.ike.ullmatcher.ha.grpc.security.GrpcClientTlsConfig;

import java.util.Objects;

public record GrpcReplicationClientConfig(
        String host,
        int port,
        int maxInboundMessageSize,
        String compressionCodec,
        int maxBatchCommands,
        int maxBatchBytes,
        GrpcClientTlsConfig tls
) {
    public GrpcReplicationClientConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(compressionCodec, "compressionCodec");
        Objects.requireNonNull(tls, "tls");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (maxInboundMessageSize <= 0 || maxBatchCommands <= 0 || maxBatchBytes <= 0) {
            throw new IllegalArgumentException("gRPC sizing values must be positive");
        }
    }

    public static GrpcReplicationClientConfig plaintext(String host, int port) {
        return new GrpcReplicationClientConfig(host, port, 4 << 20, "identity", 256, 512 << 10, GrpcClientTlsConfig.insecure());
    }
}
