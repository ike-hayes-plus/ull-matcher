package io.github.ike.ullmatcher.ha.snapshot;

import java.io.IOException;

/**
 * 本地快照物料提供者。
 */
public interface SnapshotMaterialSource {
    /**
     * 返回最近可用快照。
     *
     * @throws IOException 快照不可用时抛出
     */
    SnapshotMaterial latestSnapshot() throws IOException;
}
