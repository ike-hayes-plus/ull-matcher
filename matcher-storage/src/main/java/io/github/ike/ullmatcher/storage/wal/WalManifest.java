package io.github.ike.ullmatcher.storage.wal;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.CRC32C;

/**
 * WAL 检查点清单。
 * <p>
 * 清单把一次成功快照与当时可见的 WAL 分段、文件大小和校验和绑定起来。
 * 恢复系统可先读取清单校验本地 WAL，再从快照序列号之后重放命令。
 *
 * @param manifestFile 清单文件路径
 * @param snapshotPath 快照文件路径
 * @param snapshotSequence 快照覆盖的最后命令序列号
 * @param snapshotTradeId 快照覆盖的最后成交编号
 * @param segments WAL 分段条目
 */
public record WalManifest(Path manifestFile,
                          Path snapshotPath,
                          long snapshotSequence,
                          long snapshotTradeId,
                          List<Segment> segments) {
    /** 清单格式版本。 */
    private static final int VERSION = 1;

    /**
     * WAL 分段条目。
     *
     * @param path 分段路径
     * @param sizeBytes 文件大小
     * @param crc32c 文件 CRC32C 校验和
     */
    public record Segment(Path path, long sizeBytes, long crc32c) {}

    /**
     * 写入 WAL 检查点清单。
     *
     * @param manifestFile 清单文件路径
     * @param snapshotPath 快照文件路径
     * @param snapshotSequence 快照序列号
     * @param snapshotTradeId 快照成交编号
     * @param segments WAL 分段路径
     * @return 写入后的清单对象
     * @throws IOException 文件无法写入或校验和无法计算时抛出
     */
    public static WalManifest write(Path manifestFile, Path snapshotPath, long snapshotSequence,
                                    long snapshotTradeId, List<Path> segments) throws IOException {
        List<Segment> entries = new ArrayList<>(segments.size());
        for (Path segment : segments) {
            entries.add(new Segment(segment.toAbsolutePath().normalize(), Files.size(segment), checksum(segment)));
        }

        Files.createDirectories(manifestFile.toAbsolutePath().getParent());
        Path tmp = manifestFile.resolveSibling(manifestFile.getFileName() + ".tmp");
        String content = encode(snapshotPath.toAbsolutePath().normalize(), snapshotSequence, snapshotTradeId, entries);
        Files.writeString(tmp, content, StandardCharsets.UTF_8);
        StorageSync.forceFile(tmp);
        Files.move(tmp, manifestFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        StorageSync.forceDirectory(manifestFile.toAbsolutePath().getParent());
        return new WalManifest(manifestFile, snapshotPath, snapshotSequence, snapshotTradeId, List.copyOf(entries));
    }

    /**
     * 读取 WAL 检查点清单。
     *
     * @param manifestFile 清单文件路径
     * @return 清单对象
     * @throws IOException 清单不存在或格式非法时抛出
     */
    public static WalManifest read(Path manifestFile) throws IOException {
        Properties properties = new Properties();
        properties.load(new StringReader(Files.readString(manifestFile, StandardCharsets.UTF_8)));
        int version = Integer.parseInt(required(properties, "version"));
        if (version != VERSION) {
            throw new IOException("unsupported WAL manifest version " + version);
        }
        Path snapshotPath = Path.of(required(properties, "snapshot.path"));
        long snapshotSequence = Long.parseLong(required(properties, "snapshot.sequence"));
        long snapshotTradeId = Long.parseLong(required(properties, "snapshot.tradeId"));
        int count = Integer.parseInt(required(properties, "segment.count"));
        List<Segment> segments = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Path path = Path.of(required(properties, "segment." + i + ".path"));
            long sizeBytes = Long.parseLong(required(properties, "segment." + i + ".size"));
            long crc32c = Long.parseUnsignedLong(required(properties, "segment." + i + ".crc32c"));
            segments.add(new Segment(path, sizeBytes, crc32c));
        }
        return new WalManifest(manifestFile, snapshotPath, snapshotSequence, snapshotTradeId, List.copyOf(segments));
    }

    /**
     * 校验清单中记录的 WAL 分段仍与本地文件一致。
     *
     * @throws IOException 分段缺失或校验失败时抛出
     */
    public void validateSegments() throws IOException {
        for (Segment segment : segments) {
            if (!Files.exists(segment.path())) {
                throw new IOException("WAL segment missing: " + segment.path());
            }
            long size = Files.size(segment.path());
            if (size != segment.sizeBytes()) {
                throw new IOException("WAL segment size mismatch: " + segment.path());
            }
            long checksum = checksum(segment.path());
            if (checksum != segment.crc32c()) {
                throw new IOException("WAL segment checksum mismatch: " + segment.path());
            }
        }
    }

    /**
     * 将清单编码为稳定文本格式。
     *
     * @param snapshotPath 快照路径
     * @param snapshotSequence 快照序列号
     * @param snapshotTradeId 快照成交编号
     * @param segments 分段条目
     * @return 文本内容
     */
    private static String encode(Path snapshotPath, long snapshotSequence, long snapshotTradeId, List<Segment> segments) {
        StringBuilder out = new StringBuilder(256 + segments.size() * 160);
        out.append("version=").append(VERSION).append('\n');
        out.append("snapshot.path=").append(snapshotPath).append('\n');
        out.append("snapshot.sequence=").append(snapshotSequence).append('\n');
        out.append("snapshot.tradeId=").append(snapshotTradeId).append('\n');
        out.append("segment.count=").append(segments.size()).append('\n');
        for (int i = 0; i < segments.size(); i++) {
            Segment segment = segments.get(i);
            out.append("segment.").append(i).append(".path=").append(segment.path()).append('\n');
            out.append("segment.").append(i).append(".size=").append(segment.sizeBytes()).append('\n');
            out.append("segment.").append(i).append(".crc32c=").append(Long.toUnsignedString(segment.crc32c())).append('\n');
        }
        return out.toString();
    }

    /**
     * 读取必填属性。
     *
     * @param properties 属性集合
     * @param key 属性名
     * @return 属性值
     * @throws IOException 属性缺失时抛出
     */
    private static String required(Properties properties, String key) throws IOException {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IOException("missing WAL manifest property " + key);
        }
        return value;
    }

    /**
     * 计算文件 CRC32C 校验和。
     *
     * @param file 文件路径
     * @return 校验和值
     * @throws IOException 文件读取失败时抛出
     */
    private static long checksum(Path file) throws IOException {
        CRC32C crc = new CRC32C();
        byte[] buffer = new byte[64 * 1024];
        try (var in = Files.newInputStream(file)) {
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read > 0) crc.update(buffer, 0, read);
            }
        }
        return crc.getValue();
    }
}
