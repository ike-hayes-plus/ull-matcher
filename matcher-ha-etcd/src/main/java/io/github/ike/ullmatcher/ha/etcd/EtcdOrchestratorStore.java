package io.github.ike.ullmatcher.ha.etcd;

import io.github.ike.ullmatcher.orchestrator.OrchestratorKeySpace;
import io.github.ike.ullmatcher.orchestrator.OrchestratorPayloadCodec;
import io.github.ike.ullmatcher.orchestrator.OrchestratorStore;
import io.github.ike.ullmatcher.orchestrator.RegisteredShard;
import io.github.ike.ullmatcher.orchestrator.ShardLifecycleState;
import io.github.ike.ullmatcher.orchestrator.SymbolRoute;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * etcd 支持的 3.0 {@link OrchestratorStore}。
 */
public final class EtcdOrchestratorStore implements OrchestratorStore, Closeable {
    private final EtcdClient client;
    private final String v3Root;
    private final long leaseTtlSeconds;

    public EtcdOrchestratorStore(EtcdConfig config) throws IOException {
        this(new EtcdClient(config), OrchestratorKeySpace.v3Root(config.keyPrefix()), config.leaseTtlSeconds());
    }

    EtcdOrchestratorStore(EtcdClient client, String v3Root, long leaseTtlSeconds) {
        this.client = Objects.requireNonNull(client, "client");
        this.v3Root = Objects.requireNonNull(v3Root, "v3Root");
        if (leaseTtlSeconds <= 0L) {
            throw new IllegalArgumentException("leaseTtlSeconds must be positive");
        }
        this.leaseTtlSeconds = leaseTtlSeconds;
    }

    @Override
    public void registerShard(RegisteredShard shard) throws IOException {
        Objects.requireNonNull(shard, "shard");
        long leaseId = client.grantLease(leaseTtlSeconds);
        client.put(shardKey(shard.shardKey()), OrchestratorPayloadCodec.encodeShard(shard), leaseId);
    }

    @Override
    public void markDraining(String shardKey) throws IOException {
        RegisteredShard existing = getShard(shardKey).orElseThrow(() ->
                new IOException("unknown shardKey=" + shardKey));
        RegisteredShard draining = existing.withState(ShardLifecycleState.DRAINING, System.currentTimeMillis());
        long leaseId = client.grantLease(leaseTtlSeconds);
        client.put(shardKey(shardKey), OrchestratorPayloadCodec.encodeShard(draining), leaseId);
    }

    @Override
    public void unregisterShard(String shardKey) throws IOException {
        Optional<RegisteredShard> shard = getShard(shardKey);
        if (shard.isPresent()) {
            client.delete(OrchestratorKeySpace.symbolRouteKey(v3Root, shard.get().symbolId()));
        }
        client.delete(shardKey(shardKey));
    }

    @Override
    public void bindSymbol(int symbolId, String shardKey, long generation) throws IOException {
        if (getShard(shardKey).isEmpty()) {
            throw new IOException("unknown shardKey=" + shardKey);
        }
        long updatedAt = System.currentTimeMillis();
        client.put(
                OrchestratorKeySpace.symbolRouteKey(v3Root, symbolId),
                OrchestratorPayloadCodec.encodeSymbolRoute(symbolId, shardKey, generation, updatedAt),
                0L
        );
    }

    @Override
    public Optional<SymbolRoute> lookupRoute(int symbolId) throws IOException {
        EtcdClient.KeyValue routeValue = client.get(OrchestratorKeySpace.symbolRouteKey(v3Root, symbolId));
        if (routeValue == null) {
            return Optional.empty();
        }
        OrchestratorPayloadCodec.SymbolRouteHeader header =
                OrchestratorPayloadCodec.decodeSymbolRoute(symbolId, routeValue.value());
        RegisteredShard shard = getShard(header.shardKey()).orElse(null);
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
        EtcdClient.KeyValue value = client.get(shardKey(shardKey));
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(OrchestratorPayloadCodec.decodeShard(shardKey, value.value()));
    }

    @Override
    public List<RegisteredShard> listShards() throws IOException {
        List<EtcdClient.KeyValue> keyValues = client.rangeByPrefix(OrchestratorKeySpace.shardPrefix(v3Root));
        ArrayList<RegisteredShard> shards = new ArrayList<>(keyValues.size());
        for (EtcdClient.KeyValue keyValue : keyValues) {
            String key = keyValue.key();
            String shardKey = key.substring(OrchestratorKeySpace.shardPrefix(v3Root).length());
            shards.add(OrchestratorPayloadCodec.decodeShard(shardKey, keyValue.value()));
        }
        return shards;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private String shardKey(String shardKey) {
        return OrchestratorKeySpace.shardRecordKey(v3Root, shardKey);
    }
}
