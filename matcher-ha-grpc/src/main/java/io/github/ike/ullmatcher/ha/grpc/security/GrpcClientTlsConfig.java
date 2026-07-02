package io.github.ike.ullmatcher.ha.grpc.security;

import java.nio.file.Path;

public record GrpcClientTlsConfig(
        boolean plaintext,
        Path trustCertCollectionFile,
        Path certificateChainFile,
        Path privateKeyFile,
        String authorityOverride
) {
    public GrpcClientTlsConfig {
        if (!plaintext && trustCertCollectionFile == null) {
            throw new IllegalArgumentException("TLS client configuration requires trustCertCollectionFile");
        }
        if ((certificateChainFile == null) != (privateKeyFile == null)) {
            throw new IllegalArgumentException("certificateChainFile and privateKeyFile must be provided together");
        }
    }

    public static GrpcClientTlsConfig insecure() {
        return new GrpcClientTlsConfig(true, null, null, null, "");
    }
}
