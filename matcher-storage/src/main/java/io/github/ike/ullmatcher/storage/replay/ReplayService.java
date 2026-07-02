package io.github.ike.ullmatcher.storage.replay;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.storage.wal.WalReader;

import java.io.IOException;

/**
 * WAL 重放工具。
 * <p>
 * 从 WAL 读取命令并回放到撮合器。调用方可以传入起始序列号，跳过快照之前的命令。
 */
public final class ReplayService {
    /**
     * 工具类。
     */
    private ReplayService() {}

    /**
     * 重放指定序列边界之后的 WAL 命令。
     *
     * @param reader 顺序 WAL 读取器
     * @param matcher 接收重放命令的撮合器
     * @param fromExclusiveSequence 小于等于该序列号的命令将被跳过
     * @return 已应用命令数量
     * @throws IOException WAL 读取失败时抛出
     */
    public static long replay(WalReader reader, UltraLowLatencyMatcher matcher, long fromExclusiveSequence) throws IOException {
        long applied = 0;
        Command c;
        while ((c = reader.next()) != null) {
            if (c.sequence <= fromExclusiveSequence) continue;
            matcher.onCommand(c);
            applied++;
        }
        return applied;
    }
}
