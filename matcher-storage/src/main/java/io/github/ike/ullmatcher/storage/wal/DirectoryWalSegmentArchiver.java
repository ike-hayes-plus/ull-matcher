package io.github.ike.ullmatcher.storage.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

/**
 * 将已快照覆盖的 WAL 分段复制到本地冷备目录，布局：
 * {@code {coldRoot}/{shardKey}/{nodeId}/{segmentFileName}}。
 * <p>
 * 复制成功并 fsync 后，{@link SegmentedMmapWal} 会删除热 WAL 上的原文件。
 */
final class DirectoryWalSegmentArchiver implements WalSegmentArchiver {
    private final Path coldRoot;
    private final Path destinationDirectory;

    DirectoryWalSegmentArchiver(Path coldRoot, String shardKey, String nodeId) {
        this.coldRoot = Objects.requireNonNull(coldRoot, "coldRoot").toAbsolutePath().normalize();
        Objects.requireNonNull(shardKey, "shardKey");
        Objects.requireNonNull(nodeId, "nodeId");
        this.destinationDirectory = this.coldRoot.resolve(sanitizePathSegment(shardKey)).resolve(sanitizePathSegment(nodeId));
    }

    @Override
    public void archive(Path segmentPath) throws IOException {
        Objects.requireNonNull(segmentPath, "segmentPath");
        if (!Files.isRegularFile(segmentPath)) {
            throw new IOException("WAL segment is not a regular file: " + segmentPath);
        }
        Files.createDirectories(destinationDirectory);
        Path destination = destinationDirectory.resolve(segmentPath.getFileName());
        Files.copy(segmentPath, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
        StorageSync.forceFile(destination);
        StorageSync.forceDirectory(destinationDirectory);
        StorageSync.forceDirectory(coldRoot);
    }

    Path destinationDirectory() {
        return destinationDirectory;
    }

    private static String sanitizePathSegment(String value) {
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("path segment must not be blank");
        }
        return trimmed.replace('/', '_').replace('\\', '_');
    }
}
