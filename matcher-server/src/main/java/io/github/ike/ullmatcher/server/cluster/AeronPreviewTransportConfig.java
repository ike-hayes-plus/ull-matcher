package io.github.ike.ullmatcher.server.cluster;

import java.nio.file.Path;
import java.util.Objects;

public record AeronPreviewTransportConfig(
        Path directory,
        int port,
        int streamId
) {
    private static final int SNAPSHOT_REQUEST_PORT_OFFSET = 100;
    private static final int SNAPSHOT_RESPONSE_PORT_OFFSET = 200;
    private static final int CONTROL_REQUEST_PORT_OFFSET = 300;
    private static final int CONTROL_RESPONSE_PORT_OFFSET = 400;
    private static final int SECURITY_HANDSHAKE_REQUEST_PORT_OFFSET = 500;
    private static final int SECURITY_HANDSHAKE_RESPONSE_PORT_OFFSET = 600;
    private static final int COMMAND_ACK_PORT_OFFSET = 700;
    private static final int SNAPSHOT_REQUEST_STREAM_ID_OFFSET = 100;
    private static final int SNAPSHOT_RESPONSE_STREAM_ID_OFFSET = 200;
    private static final int CONTROL_REQUEST_STREAM_ID_OFFSET = 300;
    private static final int CONTROL_RESPONSE_STREAM_ID_OFFSET = 400;
    private static final int SECURITY_HANDSHAKE_REQUEST_STREAM_ID_OFFSET = 500;
    private static final int SECURITY_HANDSHAKE_RESPONSE_STREAM_ID_OFFSET = 600;
    private static final int COMMAND_ACK_STREAM_ID_OFFSET = 700;

    public AeronPreviewTransportConfig {
        Objects.requireNonNull(directory, "directory");
        if (port <= 0 || streamId <= 0) {
            throw new IllegalArgumentException("Aeron preview port and streamId must be positive");
        }
    }

    public String commandChannel(String host) {
        return udpChannel(host, port);
    }

    public String snapshotRequestChannel(String host) {
        return udpChannel(host, port + SNAPSHOT_REQUEST_PORT_OFFSET);
    }

    public String snapshotResponseChannel(String host) {
        return udpChannel(host, port + SNAPSHOT_RESPONSE_PORT_OFFSET);
    }

    public String controlRequestChannel(String host) {
        return udpChannel(host, port + CONTROL_REQUEST_PORT_OFFSET);
    }

    public String controlResponseChannel(String host) {
        return udpChannel(host, port + CONTROL_RESPONSE_PORT_OFFSET);
    }

    public String securityHandshakeRequestChannel(String host) {
        return udpChannel(host, port + SECURITY_HANDSHAKE_REQUEST_PORT_OFFSET);
    }

    public String securityHandshakeResponseChannel(String host) {
        return udpChannel(host, port + SECURITY_HANDSHAKE_RESPONSE_PORT_OFFSET);
    }

    public String commandAckChannel(String host) {
        return udpChannel(host, port + COMMAND_ACK_PORT_OFFSET);
    }

    public int snapshotRequestStreamId() {
        return streamId + SNAPSHOT_REQUEST_STREAM_ID_OFFSET;
    }

    public int snapshotResponseStreamId() {
        return streamId + SNAPSHOT_RESPONSE_STREAM_ID_OFFSET;
    }

    public int controlRequestStreamId() {
        return streamId + CONTROL_REQUEST_STREAM_ID_OFFSET;
    }

    public int controlResponseStreamId() {
        return streamId + CONTROL_RESPONSE_STREAM_ID_OFFSET;
    }

    public int securityHandshakeRequestStreamId() {
        return streamId + SECURITY_HANDSHAKE_REQUEST_STREAM_ID_OFFSET;
    }

    public int securityHandshakeResponseStreamId() {
        return streamId + SECURITY_HANDSHAKE_RESPONSE_STREAM_ID_OFFSET;
    }

    public int commandAckStreamId() {
        return streamId + COMMAND_ACK_STREAM_ID_OFFSET;
    }

    private static String udpChannel(String host, int port) {
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        return "aeron:udp?endpoint=" + host + ":" + port;
    }
}
