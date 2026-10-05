package io.github.ike.ullmatcher.orchestrator;

/**
 * 3.0 编排键空间（挂载在现有 {@code matcher} etcd/ZK prefix 之下）。
 */
public final class OrchestratorKeySpace {
    public static final String V3_SEGMENT = "/v3";

    private OrchestratorKeySpace() {
    }

    public static String v3Root(String deploymentKeyPrefix) {
        String normalized = deploymentKeyPrefix.endsWith("/")
                ? deploymentKeyPrefix.substring(0, deploymentKeyPrefix.length() - 1)
                : deploymentKeyPrefix;
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("deploymentKeyPrefix must not be blank");
        }
        return normalized + V3_SEGMENT;
    }

    public static String shardRecordKey(String v3Root, String shardKey) {
        return v3Root + "/shards/" + sanitizePathSegment(shardKey);
    }

    public static String symbolRouteKey(String v3Root, int symbolId) {
        if (symbolId <= 0) {
            throw new IllegalArgumentException("symbolId must be positive");
        }
        return v3Root + "/routes/symbols/" + symbolId;
    }

    public static String shardPrefix(String v3Root) {
        return v3Root + "/shards/";
    }

    static String sanitizePathSegment(String value) {
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("path segment must not be blank");
        }
        return trimmed.replace('/', '_').replace('\\', '_');
    }
}
