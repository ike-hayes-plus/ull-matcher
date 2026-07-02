package io.github.ike.ullmatcher.storage.wal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 存储持久化辅助方法。
 */
public final class StorageSync {
    /**
     * 工具类。
     */
    private StorageSync() {}

    /**
     * 强制刷新文件内容和元数据。
     *
     * @param file 文件路径
     * @throws IOException 刷新失败时抛出
     */
    public static void forceFile(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    /**
     * 尽力刷新目录项，保证原子替换后的目录元数据落盘。
     *
     * @param directory 目录路径
     * @throws IOException 刷新失败时抛出
     */
    public static void forceDirectory(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) return;
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException ignoredOnUnsupportedFileSystem) {
            // 部分文件系统不支持打开目录，快照文件本身已经完成 fsync。
        }
    }
}
