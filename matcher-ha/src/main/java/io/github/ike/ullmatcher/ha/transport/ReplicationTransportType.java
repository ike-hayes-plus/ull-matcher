package io.github.ike.ullmatcher.ha.transport;

public enum ReplicationTransportType {
    GRPC,
    AERON,
    AERON_PREVIEW;

    public boolean requiresGrpcReplicationServer() {
        return this == GRPC || this == AERON_PREVIEW;
    }
}
