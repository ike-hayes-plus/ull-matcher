package io.github.ike.ullmatcher.storage.wal;

import java.nio.file.Path;
import java.util.Objects;

/**
 * WAL 分段冷备配置。未设置 {@link #coldArchiveDirectory()} 时不归档（与 2.0 行为一致）。
 */
public record WalArchiveConfig(Path coldArchiveDirectory) {
    public WalArchiveConfig {
        if (coldArchiveDirectory != null) {
            coldArchiveDirectory = coldArchiveDirectory.toAbsolutePath().normalize();
        }
    }

    public static WalArchiveConfig disabled() {
        return new WalArchiveConfig(null);
    }

    public static WalArchiveConfig ofDirectory(Path directory) {
        Objects.requireNonNull(directory, "directory");
        if (directory.toString().isBlank()) {
            throw new IllegalArgumentException("cold archive directory must not be blank");
        }
        return new WalArchiveConfig(directory);
    }

    public static WalArchiveConfig fromProperty(String value) {
        if (value == null || value.isBlank()) {
            return disabled();
        }
        return ofDirectory(Path.of(value.trim()));
    }

    public boolean enabled() {
        return coldArchiveDirectory != null;
    }

    /**
     * 按 shard / node 分区创建目录归档器。
     */
    public WalSegmentArchiver archiver(String nodeId, String shardKey) {
        if (!enabled()) {
            return WalSegmentArchiver.noop();
        }
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(shardKey, "shardKey");
        if (nodeId.isBlank() || shardKey.isBlank()) {
            throw new IllegalArgumentException("nodeId and shardKey must not be blank when WAL archive is enabled");
        }
        return new DirectoryWalSegmentArchiver(coldArchiveDirectory, shardKey, nodeId);
    }
}
