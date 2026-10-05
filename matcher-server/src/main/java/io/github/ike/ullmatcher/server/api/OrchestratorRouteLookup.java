package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.orchestrator.SymbolRoute;

import java.io.IOException;
import java.util.Optional;

/**
 * 只读 symbol 路由查询（通常由 {@code OrchestratorShardLifecycle} 背后的 store 提供）。
 */
@FunctionalInterface
public interface OrchestratorRouteLookup {
    Optional<SymbolRoute> lookup(int symbolId) throws IOException;
}
