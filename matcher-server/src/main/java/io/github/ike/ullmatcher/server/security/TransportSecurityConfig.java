package io.github.ike.ullmatcher.server.security;

import java.nio.file.Path;
import java.util.Objects;

/**
 * 描述传输层证书、私钥、信任链配置。
 */
public record TransportSecurityConfig(
        Path certificateChainFile,
        Path privateKeyFile,
        Path trustCertCollectionFile,
        boolean requireMutualTls
) {
    public TransportSecurityConfig {
        Objects.requireNonNull(certificateChainFile, "certificateChainFile");
        Objects.requireNonNull(privateKeyFile, "privateKeyFile");
        if (requireMutualTls && trustCertCollectionFile == null) {
            throw new IllegalArgumentException("mutual transport authentication requires trustCertCollectionFile");
        }
    }
}
