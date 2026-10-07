package io.github.ike.ullmatcher.ha.transport;

public enum ReplicationTransportType {
    GRPC,
    AERON;

    public boolean requiresGrpcReplicationServer() {
        return this == GRPC;
    }
}
