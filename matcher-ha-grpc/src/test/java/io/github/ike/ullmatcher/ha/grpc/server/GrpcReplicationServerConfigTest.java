package io.github.ike.ullmatcher.ha.grpc.server;

import io.github.ike.ullmatcher.ha.grpc.security.GrpcServerTlsConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class GrpcReplicationServerConfigTest {
    @Test
    void defaultsBindLoopbackWithPlaintextIdentityCodec() {
        GrpcReplicationServerConfig config = GrpcReplicationServerConfig.defaults(9123);

        assertEquals("127.0.0.1", config.bindHost());
        assertEquals(9123, config.port());
        assertEquals(4 << 20, config.maxInboundMessageSize());
        assertEquals(30L, config.permitKeepAliveTimeSeconds());
        assertEquals(2_000L, config.replicationIngressTimeoutMillis());
        assertEquals("identity", config.compressionCodec());
        assertNull(config.tls(), "defaults must stay plaintext so local labs need no PKI");
    }

    @Test
    void withBindHostKeepsEveryOtherSetting() {
        GrpcServerTlsConfig tls = new GrpcServerTlsConfig(Path.of("cert.pem"), Path.of("key.pem"), null, false);
        GrpcReplicationServerConfig config = new GrpcReplicationServerConfig(
                "127.0.0.1", 9123, 1 << 20, 5L, 750L, "gzip", tls);

        GrpcReplicationServerConfig rebound = config.withBindHost("0.0.0.0");

        assertEquals("0.0.0.0", rebound.bindHost());
        assertEquals(9123, rebound.port());
        assertEquals(1 << 20, rebound.maxInboundMessageSize());
        assertEquals(5L, rebound.permitKeepAliveTimeSeconds());
        assertEquals(750L, rebound.replicationIngressTimeoutMillis());
        assertEquals("gzip", rebound.compressionCodec());
        assertEquals(tls, rebound.tls());
        assertEquals(tls, GrpcReplicationServerConfig.defaults(1).withTls(tls).tls());
    }

    @Test
    void ephemeralPortZeroIsAccepted() {
        assertEquals(0, GrpcReplicationServerConfig.defaults(0).port());
    }

    @Test
    void rejectsBlankBindHostAndCompressionCodec() {
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig(null, 9123, 1 << 20, 30L, 2_000L, "identity", null));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig(" ", 9123, 1 << 20, 30L, 2_000L, "identity", null));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig("127.0.0.1", 9123, 1 << 20, 30L, 2_000L, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig("127.0.0.1", 9123, 1 << 20, 30L, 2_000L, " ", null));
    }

    @Test
    void rejectsOutOfRangePortAndNonPositiveSizing() {
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig("127.0.0.1", -1, 1 << 20, 30L, 2_000L, "identity", null));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig("127.0.0.1", 65_536, 1 << 20, 30L, 2_000L, "identity", null));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig("127.0.0.1", 9123, 0, 30L, 2_000L, "identity", null));
    }

    @Test
    void rejectsNegativeKeepAliveAndNonPositiveIngressTimeout() {
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig("127.0.0.1", 9123, 1 << 20, -1L, 2_000L, "identity", null));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationServerConfig("127.0.0.1", 9123, 1 << 20, 30L, 0L, "identity", null));
    }
}
