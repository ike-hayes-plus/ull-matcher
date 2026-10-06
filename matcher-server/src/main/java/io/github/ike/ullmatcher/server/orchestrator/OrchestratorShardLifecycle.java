package io.github.ike.ullmatcher.server.orchestrator;

import io.github.ike.ullmatcher.orchestrator.OrchestratorStore;
import io.github.ike.ullmatcher.orchestrator.RegisteredShard;
import io.github.ike.ullmatcher.orchestrator.ShardEndpoints;
import io.github.ike.ullmatcher.orchestrator.ShardLifecycleState;
import io.github.ike.ullmatcher.orchestrator.SymbolRoute;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 进程启动后在编排面注册 shard + symbol 路由，并周期性刷新 lease；关闭时删除 shard 与 symbol 路由。
 */
public final class OrchestratorShardLifecycle implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OrchestratorShardLifecycle.class);

    private final MatcherServerConfig serverConfig;
    private final OrchestratorStore store;
    private final long generation;
    private final long heartbeatIntervalMillis;
    private ScheduledExecutorService heartbeatExecutor;
    private volatile ShardEndpoints advertisedEndpoints;

    public OrchestratorShardLifecycle(MatcherServerConfig serverConfig,
                                      OrchestratorStore store,
                                      long generation,
                                      long heartbeatIntervalMillis) {
        this.serverConfig = Objects.requireNonNull(serverConfig, "serverConfig");
        this.store = Objects.requireNonNull(store, "store");
        if (generation < 0L) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        this.generation = generation;
        if (heartbeatIntervalMillis <= 0L) {
            throw new IllegalArgumentException("heartbeatIntervalMillis must be positive");
        }
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    public Optional<SymbolRoute> lookupRoute(int symbolId) throws IOException {
        return store.lookupRoute(symbolId);
    }

    public void registerAdvertisedEndpoints(String httpHost, int httpPort, int grpcPort, int binaryIngressPort)
            throws IOException {
        this.advertisedEndpoints = new ShardEndpoints(httpHost, httpPort, grpcPort, binaryIngressPort);
        publishActiveRegistration();
        startHeartbeatIfNeeded();
    }

    private void publishActiveRegistration() throws IOException {
        ShardEndpoints endpoints = advertisedEndpoints;
        if (endpoints == null) {
            throw new IllegalStateException("orchestrator endpoints not configured");
        }
        int symbolId = serverConfig.matcherConfig().symbolId();
        RegisteredShard shard = new RegisteredShard(
                serverConfig.shardKey(),
                symbolId,
                serverConfig.nodeId(),
                endpoints,
                ShardLifecycleState.ACTIVE,
                generation,
                System.currentTimeMillis()
        );
        store.registerShard(shard);
        store.bindSymbol(symbolId, serverConfig.shardKey(), generation);
        LOG.info("orchestrator registered shardKey={} symbolId={} nodeId={} http={}:{} grpc={} binary={}",
                serverConfig.shardKey(),
                symbolId,
                serverConfig.nodeId(),
                endpoints.httpHost(),
                endpoints.httpPort(),
                endpoints.grpcPort(),
                endpoints.binaryIngressPort());
    }

    private void startHeartbeatIfNeeded() {
        if (heartbeatExecutor != null) {
            return;
        }
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "orchestrator-heartbeat-" + serverConfig.nodeId());
            thread.setDaemon(true);
            return thread;
        });
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                publishActiveRegistration();
            } catch (IOException | RuntimeException e) {
                LOG.warn("orchestrator heartbeat failed nodeId={} shardKey={}",
                        serverConfig.nodeId(), serverConfig.shardKey(), e);
            }
        }, heartbeatIntervalMillis, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
            heartbeatExecutor = null;
        }
        try {
            store.unregisterShard(serverConfig.shardKey());
            LOG.info("orchestrator unregistered shardKey={} nodeId={}",
                    serverConfig.shardKey(), serverConfig.nodeId());
        } catch (IOException e) {
            LOG.warn("orchestrator unregister failed shardKey={}", serverConfig.shardKey(), e);
        }
        store.close();
    }
}
