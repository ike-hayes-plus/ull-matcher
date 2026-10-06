package io.github.ike.ullmatcher.orchestrator;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 只读路由视图：从 {@link OrchestratorStore} 刷新本地缓存，供 SDK / 网关查询。
 */
public final class RoutingTable {
    private final OrchestratorStore store;
    private volatile Map<Integer, SymbolRoute> routesBySymbol = Map.of();
    private volatile Map<String, RegisteredShard> shardsByKey = Map.of();
    private final AtomicReference<ScheduledExecutorService> backgroundRefresh = new AtomicReference<>();

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

    /**
     * 网关 / SDK 嵌入场景：按固定间隔从 {@link OrchestratorStore} 拉取路由，避免每请求打 etcd。
     */
    public void startBackgroundRefresh(long intervalMillis) {
        if (intervalMillis <= 0L) {
            throw new IllegalArgumentException("intervalMillis must be positive");
        }
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "orchestrator-routing-refresh");
            thread.setDaemon(true);
            return thread;
        });
        if (!backgroundRefresh.compareAndSet(null, executor)) {
            executor.shutdownNow();
            return;
        }
        executor.scheduleAtFixedRate(() -> {
            try {
                refresh();
            } catch (IOException ignored) {
                // next tick retries
            }
        }, 0L, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stopBackgroundRefresh() {
        ScheduledExecutorService executor = backgroundRefresh.getAndSet(null);
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
