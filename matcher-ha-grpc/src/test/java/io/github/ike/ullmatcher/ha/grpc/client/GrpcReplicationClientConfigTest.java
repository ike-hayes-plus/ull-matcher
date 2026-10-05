package io.github.ike.ullmatcher.ha.grpc.client;

import io.github.ike.ullmatcher.ha.grpc.security.GrpcClientTlsConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GrpcReplicationClientConfigTest {
    @Test
    void plaintextFactoryUsesInsecureTlsAndStreamingDefaults() {
        GrpcReplicationClientConfig config = GrpcReplicationClientConfig.plaintext("standby-a", 9123);

        assertEquals("standby-a", config.host());
        assertEquals(9123, config.port());
        assertEquals(4 << 20, config.maxInboundMessageSize());
        assertEquals("identity", config.compressionCodec());
        assertEquals(256, config.maxBatchCommands());
        assertEquals(512 << 10, config.maxBatchBytes());
        assertTrue(config.tls().plaintext());
    }

    @Test
    void rejectsMissingHostCodecOrTls() {
        GrpcClientTlsConfig tls = GrpcClientTlsConfig.insecure();

        assertThrows(NullPointerException.class,
                () -> new GrpcReplicationClientConfig(null, 9123, 1 << 20, "identity", 256, 1 << 10, tls));
        assertThrows(NullPointerException.class,
                () -> new GrpcReplicationClientConfig("standby-a", 9123, 1 << 20, null, 256, 1 << 10, tls));
        assertThrows(NullPointerException.class,
                () -> new GrpcReplicationClientConfig("standby-a", 9123, 1 << 20, "identity", 256, 1 << 10, null));
    }

    @Test
    void rejectsBlankHostAndOutOfRangePort() {
        GrpcClientTlsConfig tls = GrpcClientTlsConfig.insecure();

        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationClientConfig(" ", 9123, 1 << 20, "identity", 256, 1 << 10, tls));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationClientConfig("standby-a", 0, 1 << 20, "identity", 256, 1 << 10, tls));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationClientConfig("standby-a", 65_536, 1 << 20, "identity", 256, 1 << 10, tls));
    }

    @Test
    void rejectsNonPositiveSizingValues() {
        GrpcClientTlsConfig tls = GrpcClientTlsConfig.insecure();

        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationClientConfig("standby-a", 9123, 0, "identity", 256, 1 << 10, tls));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationClientConfig("standby-a", 9123, 1 << 20, "identity", 0, 1 << 10, tls));
        assertThrows(IllegalArgumentException.class,
                () -> new GrpcReplicationClientConfig("standby-a", 9123, 1 << 20, "identity", 256, 0, tls));
    }
}
