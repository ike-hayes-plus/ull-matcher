package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * 负责复制传输模式的运行时约束校验。
 * 包括模式锁、切换窗口以及生产环境限制。
 */
public final class ReplicationTransportPolicyEnforcer {
    private static final String LOCK_FILE_NAME = "replication-transport.lock";
    private static final String PROP_TRANSPORT = "transport";
    private static final String PROP_WINDOW_ID = "windowId";

    private ReplicationTransportPolicyEnforcer() {
    }

    public static void validateAndLock(MatcherServerConfig config) throws IOException {
        Objects.requireNonNull(config, "config");
        MatcherClusterConfig clusterConfig = config.clusterConfig();
        if (clusterConfig == null) {
            return;
        }
        ReplicationTransportPolicyConfig policy = clusterConfig.replicationTransportPolicyConfig();
        if (config.serverMode() == MatcherServerMode.PROD
                && clusterConfig.replicationTransportType() == ReplicationTransportType.AERON_PREVIEW
                && !policy.allowPreviewTransportInProd()) {
            throw new IllegalStateException(
                    "prod mode forbids matcher.replicationTransport=AERON_PREVIEW unless matcher.allowPreviewTransportInProd=true");
        }
        Path lockFile = config.walDirectory().resolveSibling(LOCK_FILE_NAME);
        Files.createDirectories(lockFile.getParent());
        if (!Files.exists(lockFile)) {
            writeLock(lockFile, clusterConfig.replicationTransportType().name(), policy.transportChangeWindowId());
            return;
        }
        Properties properties = readLock(lockFile);
        String currentTransport = properties.getProperty(PROP_TRANSPORT, "").trim();
        String desiredTransport = clusterConfig.replicationTransportType().name();
        if (currentTransport.isBlank() || currentTransport.equals(desiredTransport)) {
            writeLock(lockFile, desiredTransport, policy.transportChangeWindowId());
            return;
        }
        if (!policy.changeWindowActive()) {
            throw new IllegalStateException(
                    "replication transport change from " + currentTransport + " to " + desiredTransport
                            + " requires matcher.allowTransportChange=true and matcher.transportChangeWindowId");
        }
        writeLock(lockFile, desiredTransport, policy.transportChangeWindowId());
    }

    private static Properties readLock(Path lockFile) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(lockFile)) {
            properties.load(in);
        }
        return properties;
    }

    private static void writeLock(Path lockFile, String transport, String windowId) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(PROP_TRANSPORT, transport);
        properties.setProperty(PROP_WINDOW_ID, windowId);
        try (OutputStream out = Files.newOutputStream(lockFile)) {
            properties.store(out, "ull-matcher replication transport lock");
        }
    }
}
