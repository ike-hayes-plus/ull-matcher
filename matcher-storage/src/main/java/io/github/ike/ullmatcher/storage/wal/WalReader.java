package io.github.ike.ullmatcher.storage.wal;

import io.github.ike.ullmatcher.api.Command;

import java.io.Closeable;
import java.io.IOException;

/** WAL 顺序读取接口，用于宕机恢复时重放命令。 */
public interface WalReader extends Closeable {
    /**
     * 从日志读取下一条完整命令。
     *
     * @return 下一条命令；没有完整记录时返回 {@code null}
     * @throws IOException 命令无法读取时抛出
     */
    Command next() throws IOException;
}
