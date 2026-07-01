package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.ReplicationTransportProvider;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;

import java.io.IOException;
import java.util.Objects;

/**
 * 按复制传输类型创建具体 provider。
 * 这里集中收口装配逻辑，避免上层直接依赖具体传输实现。
 */
public final class ReplicationTransportProviders {
    private ReplicationTransportProviders() {
    }

    public static ReplicationTransportProvider create(MatcherClusterConfig clusterConfig,
                                                      ServerSecurityConfig securityConfig,
                                                      MatcherNodeService nodeService) throws IOException {
        Objects.requireNonNull(clusterConfig, "clusterConfig");
        Objects.requireNonNull(securityConfig, "securityConfig");
        Objects.requireNonNull(nodeService, "nodeService");
        return switch (clusterConfig.replicationTransportType()) {
            case GRPC -> new GrpcReplicationTransportProvider(securityConfig);
            case AERON -> new AeronReplicationTransportProvider(
                    securityConfig,
                    nodeService.currentState().nodeId(),
                    clusterConfig.advertisedHost(),
                    clusterConfig.aeronPreviewTransportConfig(),
                    nodeService::standbySyncService,
                    nodeService,
                    nodeService
            );
            case AERON_PREVIEW -> new AeronPreviewReplicationTransportProvider(
                    securityConfig,
                    clusterConfig.advertisedHost(),
                    clusterConfig.aeronPreviewTransportConfig(),
                    () -> nodeService.currentState().cursor().lastReceivedSequence()
            );
        };
    }
}
