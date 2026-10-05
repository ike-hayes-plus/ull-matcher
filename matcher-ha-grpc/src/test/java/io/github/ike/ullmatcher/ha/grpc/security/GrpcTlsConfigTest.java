package io.github.ike.ullmatcher.ha.grpc.security;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GrpcTlsConfigTest {
    private static final Path CERT = Path.of("cert.pem");
    private static final Path KEY = Path.of("key.pem");
    private static final Path TRUST = Path.of("ca.pem");

    @Test
    void insecureClientConfigDisablesTlsEntirely() {
        GrpcClientTlsConfig config = GrpcClientTlsConfig.insecure();

        assertTrue(config.plaintext());
        assertNull(config.trustCertCollectionFile());
        assertNull(config.certificateChainFile());
        assertNull(config.privateKeyFile());
        assertEquals("", config.authorityOverride());
    }

    @Test
    void tlsClientConfigAcceptsTrustOnlyAndMutualMaterial() {
        GrpcClientTlsConfig trustOnly = new GrpcClientTlsConfig(false, TRUST, null, null, "standby.internal");
        GrpcClientTlsConfig mutual = new GrpcClientTlsConfig(false, TRUST, CERT, KEY, "");

        assertFalse(trustOnly.plaintext());
        assertEquals("standby.internal", trustOnly.authorityOverride());
        assertEquals(CERT, mutual.certificateChainFile());
        assertEquals(KEY, mutual.privateKeyFile());
    }

    @Test
    void tlsClientConfigRequiresATrustStore() {
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcClientTlsConfig(false, null, null, null, ""));
    }

    @Test
    void clientKeyMaterialMustBeProvidedAsAPair() {
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcClientTlsConfig(false, TRUST, CERT, null, ""));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcClientTlsConfig(false, TRUST, null, KEY, ""));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcClientTlsConfig(true, null, CERT, null, ""));
    }

    @Test
    void serverConfigRequiresKeyMaterial() {
        assertThrows(NullPointerException.class, () -> new GrpcServerTlsConfig(null, KEY, TRUST, false));
        assertThrows(NullPointerException.class, () -> new GrpcServerTlsConfig(CERT, null, TRUST, false));
    }

    @Test
    void mutualTlsServerConfigRequiresATrustStore() {
        assertThrows(IllegalArgumentException.class, () -> new GrpcServerTlsConfig(CERT, KEY, null, true));

        GrpcServerTlsConfig mutual = new GrpcServerTlsConfig(CERT, KEY, TRUST, true);
        assertTrue(mutual.requireMutualTls());
        assertEquals(TRUST, mutual.trustCertCollectionFile());
    }

    @Test
    void oneWayServerConfigMayOmitTheTrustStore() {
        GrpcServerTlsConfig oneWay = new GrpcServerTlsConfig(CERT, KEY, null, false);

        assertFalse(oneWay.requireMutualTls());
        assertNull(oneWay.trustCertCollectionFile());
        assertEquals(CERT, oneWay.certificateChainFile());
        assertEquals(KEY, oneWay.privateKeyFile());
    }
}
