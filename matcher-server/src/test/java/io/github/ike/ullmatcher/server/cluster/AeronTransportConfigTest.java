package io.github.ike.ullmatcher.server.cluster;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class AeronTransportConfigTest {
    @Test
    void channelsDeriveDistinctPortsFromTheBasePort() {
        AeronTransportConfig config = new AeronTransportConfig(Path.of("target", "aeron"), 15_000, 11_000);

        assertEquals("aeron:udp?endpoint=127.0.0.1:15000", config.commandChannel("127.0.0.1"));
        assertEquals("aeron:udp?endpoint=127.0.0.1:15100", config.snapshotRequestChannel("127.0.0.1"));
        assertEquals("aeron:udp?endpoint=127.0.0.1:15200", config.snapshotResponseChannel("127.0.0.1"));
        assertEquals("aeron:udp?endpoint=127.0.0.1:15300", config.controlRequestChannel("127.0.0.1"));
        assertEquals("aeron:udp?endpoint=127.0.0.1:15400", config.controlResponseChannel("127.0.0.1"));
        assertEquals("aeron:udp?endpoint=127.0.0.1:15500", config.securityHandshakeRequestChannel("127.0.0.1"));
        assertEquals("aeron:udp?endpoint=127.0.0.1:15600", config.securityHandshakeResponseChannel("127.0.0.1"));
        assertEquals("aeron:udp?endpoint=127.0.0.1:15700", config.commandAckChannel("127.0.0.1"));
    }

    @Test
    void streamIdsDeriveDistinctValuesFromTheBaseStreamId() {
        AeronTransportConfig config = new AeronTransportConfig(Path.of("target", "aeron"), 15_000, 11_000);

        assertEquals(11_000, config.streamId());
        assertEquals(11_100, config.snapshotRequestStreamId());
        assertEquals(11_200, config.snapshotResponseStreamId());
        assertEquals(11_300, config.controlRequestStreamId());
        assertEquals(11_400, config.controlResponseStreamId());
        assertEquals(11_500, config.securityHandshakeRequestStreamId());
        assertEquals(11_600, config.securityHandshakeResponseStreamId());
        assertEquals(11_700, config.commandAckStreamId());
    }

    @Test
    void nonPositivePortOrStreamIdIsRejected() {
        Path directory = Path.of("target", "aeron");

        assertEquals("Aeron port and streamId must be positive",
                assertThrows(IllegalArgumentException.class,
                        () -> new AeronTransportConfig(directory, 0, 11_000)).getMessage());
        assertEquals("Aeron port and streamId must be positive",
                assertThrows(IllegalArgumentException.class,
                        () -> new AeronTransportConfig(directory, 15_000, 0)).getMessage());
        assertThrows(NullPointerException.class, () -> new AeronTransportConfig(null, 15_000, 11_000));
    }

    @Test
    void blankHostIsRejectedWhenBuildingAChannel() {
        AeronTransportConfig config = new AeronTransportConfig(Path.of("target", "aeron"), 15_000, 11_000);

        assertEquals("host must not be blank",
                assertThrows(IllegalArgumentException.class, () -> config.commandChannel(" ")).getMessage());
        assertThrows(NullPointerException.class, () -> config.commandChannel(null));
    }
}
