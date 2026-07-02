package io.github.ike.ullmatcher.server.cluster;

import java.util.Objects;

/**
 * 描述复制传输切换与生产约束策略。
 */
public record ReplicationTransportPolicyConfig(
        boolean allowTransportChange,
        String transportChangeWindowId,
        boolean allowPreviewTransportInProd
) {
    public ReplicationTransportPolicyConfig {
        Objects.requireNonNull(transportChangeWindowId, "transportChangeWindowId");
    }

    public static ReplicationTransportPolicyConfig defaults() {
        return new ReplicationTransportPolicyConfig(false, "", false);
    }

    public boolean changeWindowActive() {
        return allowTransportChange && !transportChangeWindowId.isBlank();
    }
}
