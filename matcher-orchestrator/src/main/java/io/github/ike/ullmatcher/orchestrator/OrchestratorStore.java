package io.github.ike.ullmatcher.orchestrator;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * 3.0 分片注册与 symbol 路由的存储抽象。
 */
public interface OrchestratorStore extends AutoCloseable {
    /**
     * 注册或心跳刷新分片记录（实现方可用 lease 保活）。
     */
    void registerShard(RegisteredShard shard) throws IOException;

    /**
     * 将分片标记为 drain（不再接受新路由流量，已有连接由节点自行处理）。
     */
    void markDraining(String shardKey) throws IOException;

    void unregisterShard(String shardKey) throws IOException;

    /**
     * 绑定 symbol 到 shard（3.0 为 1:1）。
     */
    void bindSymbol(int symbolId, String shardKey, long generation) throws IOException;

    Optional<SymbolRoute> lookupRoute(int symbolId) throws IOException;

    Optional<RegisteredShard> getShard(String shardKey) throws IOException;

    List<RegisteredShard> listShards() throws IOException;

    @Override
    void close() throws IOException;
}
