package io.github.ike.ullmatcher.server.cluster;

public enum ReplicationTransportType {
    GRPC,
    AERON,
    AERON_PREVIEW;

    public boolean requiresGrpcReplicationServer() {
        return this == GRPC || this == AERON_PREVIEW;
    }
}
