package io.github.ike.ullmatcher.storage.wal;

import io.github.ike.ullmatcher.api.Command;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * WAL 写入接口。
 * <p>
 * 生产用法：先追加 WAL，成功后再把命令投递给撮合线程。
 * 这样机器宕机时可以通过快照和 WAL 重放恢复订单簿。
 */
public interface WalWriter extends Closeable {
    /**
     * 向日志追加一条命令。
     *
     * @param command 待持久化命令
     * @throws IOException 命令无法写入时抛出
     */
    void append(Command command) throws IOException;

    /**
     * 向日志顺序追加多条命令。
     *
     * @param commands 待持久化命令序列
     * @throws IOException 任一命令无法写入时抛出
     */
    default void appendAll(Iterable<Command> commands) throws IOException {
        Objects.requireNonNull(commands, "commands");
        for (Command command : commands) {
            append(Objects.requireNonNull(command, "command"));
        }
    }

    /**
     * 按实现约定将缓冲的 WAL 数据强制写入持久化存储。
     *
     * @throws IOException 数据无法强制写入时抛出
     */
    void force() throws IOException;
}
