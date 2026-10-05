package io.github.ike.ullmatcher.orchestrator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内编排存储，用于单元测试与无控制面 lab。
 */
public final class InMemoryOrchestratorStore implements OrchestratorStore {
    private final Map<String, RegisteredShard> shards = new ConcurrentHashMap<>();
    private final Map<Integer, OrchestratorPayloadCodec.SymbolRouteHeader> symbolRoutes = new ConcurrentHashMap<>();

    @Override
    public void registerShard(RegisteredShard shard) throws IOException {
        Objects.requireNonNull(shard, "shard");
        shards.put(shard.shardKey(), shard);
    }

    @Override
    public void markDraining(String shardKey) throws IOException {
        RegisteredShard existing = shards.get(shardKey);
        if (existing == null) {
            throw new IOException("unknown shardKey=" + shardKey);
        }
        shards.put(shardKey, existing.withState(ShardLifecycleState.DRAINING, System.currentTimeMillis()));
    }

    @Override
    public void unregisterShard(String shardKey) throws IOException {
        shards.remove(shardKey);
        symbolRoutes.entrySet().removeIf(entry -> entry.getValue().shardKey().equals(shardKey));
    }

    @Override
    public void bindSymbol(int symbolId, String shardKey, long generation) throws IOException {
        if (!shards.containsKey(shardKey)) {
            throw new IOException("unknown shardKey=" + shardKey);
        }
        symbolRoutes.put(symbolId, new OrchestratorPayloadCodec.SymbolRouteHeader(
                symbolId,
                shardKey,
                generation,
                System.currentTimeMillis()
        ));
    }

    @Override
    public Optional<SymbolRoute> lookupRoute(int symbolId) throws IOException {
        OrchestratorPayloadCodec.SymbolRouteHeader header = symbolRoutes.get(symbolId);
        if (header == null) {
            return Optional.empty();
        }
        RegisteredShard shard = shards.get(header.shardKey());
        return Optional.of(new SymbolRoute(
                header.symbolId(),
                header.shardKey(),
                header.generation(),
                header.updatedAtEpochMillis(),
                shard
        ));
    }

    @Override
    public Optional<RegisteredShard> getShard(String shardKey) throws IOException {
        return Optional.ofNullable(shards.get(shardKey));
    }

    @Override
    public List<RegisteredShard> listShards() throws IOException {
        return new ArrayList<>(shards.values());
    }

    @Override
    public void close() {
    }

    /** 测试用：当前 symbol 绑定快照。 */
    Map<Integer, OrchestratorPayloadCodec.SymbolRouteHeader> symbolRoutesSnapshot() {
        return Map.copyOf(new LinkedHashMap<>(symbolRoutes));
    }
}
