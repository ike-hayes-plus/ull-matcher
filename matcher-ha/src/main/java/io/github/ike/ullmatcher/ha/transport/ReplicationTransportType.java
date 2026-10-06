package io.github.ike.ullmatcher.ha.transport;

public enum ReplicationTransportType {
    GRPC,
    AERON,
    /** @deprecated lab 预览路径，合并进 {@link #AERON} profile 或后续移除；见 replication-transport ADR */
    @Deprecated(since = "3.0", forRemoval = true)
    AERON_PREVIEW;

    public boolean requiresGrpcReplicationServer() {
        return this == GRPC || this == AERON_PREVIEW;
    }
}
