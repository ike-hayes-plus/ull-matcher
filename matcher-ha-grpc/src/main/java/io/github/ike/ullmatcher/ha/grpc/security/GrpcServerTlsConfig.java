package io.github.ike.ullmatcher.ha.grpc.security;

import java.nio.file.Path;
import java.util.Objects;

public record GrpcServerTlsConfig(
        Path certificateChainFile,
        Path privateKeyFile,
        Path trustCertCollectionFile,
        boolean requireMutualTls
) {
    public GrpcServerTlsConfig {
        Objects.requireNonNull(certificateChainFile, "certificateChainFile");
        Objects.requireNonNull(privateKeyFile, "privateKeyFile");
        if (requireMutualTls && trustCertCollectionFile == null) {
            throw new IllegalArgumentException("mTLS requires trustCertCollectionFile");
        }
    }
}
