package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.server.security.TlsReloadSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * 定义主备复制传输的统一装配契约。
 * 实现方负责暴露节点元数据、创建对端客户端，并汇总本地传输观测信息。
 */
public interface ReplicationTransportProvider extends Closeable {
    String TRANSPORT_METADATA_KEY = "replicationTransport";
    String TRANSPORT_CHANGE_WINDOW_METADATA_KEY = "replicationTransportChangeWindowId";
    String AERON_CHANNEL_METADATA_KEY = "aeronChannel";
    String AERON_STREAM_ID_METADATA_KEY = "aeronStreamId";
    String AERON_SNAPSHOT_REQUEST_CHANNEL_METADATA_KEY = "aeronSnapshotRequestChannel";
    String AERON_SNAPSHOT_REQUEST_STREAM_ID_METADATA_KEY = "aeronSnapshotRequestStreamId";
    String AERON_CONTROL_REQUEST_CHANNEL_METADATA_KEY = "aeronControlRequestChannel";
    String AERON_CONTROL_REQUEST_STREAM_ID_METADATA_KEY = "aeronControlRequestStreamId";
    String AERON_SECURITY_HANDSHAKE_REQUEST_CHANNEL_METADATA_KEY = "aeronSecurityHandshakeRequestChannel";
    String AERON_SECURITY_HANDSHAKE_REQUEST_STREAM_ID_METADATA_KEY = "aeronSecurityHandshakeRequestStreamId";

    ReplicationTransportType type();

    Map<String, String> localNodeMetadata();

    ClusterPeerClient connect(DiscoveredNode node) throws IOException;

    TransportMetricsSnapshot metricsSnapshot();

    default TlsReloadSnapshot securitySnapshot() {
        return new TlsReloadSnapshot(0L, 0L, 0L, false, "");
    }
}
