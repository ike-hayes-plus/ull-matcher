package io.github.ike.ullmatcher.ha.transport;

public enum ReplicationTransportType {
    GRPC,
    AERON,
    /** @deprecated 2.1 起废弃，合并进 {@link #AERON} lab profile；见 doc/architecture/replication-transport-2.1.md */
    @Deprecated(since = "2.1", forRemoval = true)
    AERON_PREVIEW;

    public boolean requiresGrpcReplicationServer() {
        return this == GRPC || this == AERON_PREVIEW;
    }
}
