package io.github.ike.ullmatcher.orchestrator;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 只读路由视图：从 {@link OrchestratorStore} 刷新本地缓存，供 SDK / 网关查询。
 */
public final class RoutingTable {
    private final OrchestratorStore store;
    private volatile Map<Integer, SymbolRoute> routesBySymbol = Map.of();
    private volatile Map<String, RegisteredShard> shardsByKey = Map.of();

    public RoutingTable(OrchestratorStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    public void refresh() throws IOException {
        Map<Integer, SymbolRoute> nextRoutes = new LinkedHashMap<>();
        Map<String, RegisteredShard> nextShards = new LinkedHashMap<>();
        for (RegisteredShard shard : store.listShards()) {
            nextShards.put(shard.shardKey(), shard);
        }
        for (RegisteredShard shard : nextShards.values()) {
            Optional<SymbolRoute> route = store.lookupRoute(shard.symbolId());
            route.ifPresent(symbolRoute -> nextRoutes.put(symbolRoute.symbolId(), symbolRoute));
        }
        this.routesBySymbol = Map.copyOf(nextRoutes);
        this.shardsByKey = Map.copyOf(nextShards);
    }

    public Optional<SymbolRoute> route(int symbolId) {
        return Optional.ofNullable(routesBySymbol.get(symbolId));
    }

    public Optional<RegisteredShard> shard(String shardKey) {
        return Optional.ofNullable(shardsByKey.get(shardKey));
    }

    public Map<Integer, SymbolRoute> routesSnapshot() {
        return routesBySymbol;
    }
}
