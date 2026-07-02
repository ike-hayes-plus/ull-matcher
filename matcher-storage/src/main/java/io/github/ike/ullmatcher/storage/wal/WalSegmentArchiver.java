package io.github.ike.ullmatcher.storage.wal;

import java.io.IOException;
import java.nio.file.Path;

/**
 * WAL 分段归档扩展点。
 * <p>
 * SDK 不绑定具体对象存储客户端。业务服务可以在这里接入 S3、OSS、NFS 或其他归档介质。
 */
@FunctionalInterface
public interface WalSegmentArchiver {
    /**
     * 归档一个已经被快照覆盖、即将从本地删除的 WAL 分段。
     *
     * @param segmentPath 本地 WAL 分段路径
     * @throws IOException 归档失败时抛出，调用方应保留本地文件
     */
    void archive(Path segmentPath) throws IOException;

    /**
     * 返回不执行任何归档操作的实现。
     *
     * @return 空归档器
     */
    static WalSegmentArchiver noop() {
        return segmentPath -> {};
    }
}
