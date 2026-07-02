package io.github.ike.ullmatcher.server.security;

import java.security.PrivateKey;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;

/**
 * 承载一次成功加载后的传输安全材料快照。
 */
public record TransportSecurityMaterials(
        long generation,
        X509Certificate leafCertificate,
        List<X509Certificate> certificateChain,
        List<byte[]> certificateChainDer,
        PrivateKey privateKey,
        Set<TrustAnchor> trustAnchors,
        String certificateFingerprint
) {
}
