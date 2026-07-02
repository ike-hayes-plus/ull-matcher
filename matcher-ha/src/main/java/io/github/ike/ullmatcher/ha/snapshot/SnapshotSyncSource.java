package io.github.ike.ullmatcher.ha.snapshot;

import java.io.IOException;
import java.nio.file.Path;

/**
 * 远端快照同步源。
 */
public interface SnapshotSyncSource {
    /**
     * 目标节点标识。
     */
    String nodeId();

    /**
     * 下载远端最新快照到本地路径。
     *
     * @param targetFile 本地目标文件
     * @param timeoutNanos 下载超时预算
     * @return 下载结果
     * @throws IOException 下载失败时抛出
     */
    SnapshotSyncResult downloadLatestSnapshot(Path targetFile, long timeoutNanos) throws IOException;
}
