package io.github.ike.ullmatcher.orchestrator;

import java.util.Objects;

/**
 * 编排 KV 值的稳定线型编码（避免在 orchestrator 模块引入 JSON 依赖）。
 */
public final class OrchestratorPayloadCodec {
    private static final String FIELD = "|";

    private OrchestratorPayloadCodec() {
    }

    public static String encodeShard(RegisteredShard shard) {
        Objects.requireNonNull(shard, "shard");
        ShardEndpoints e = shard.endpoints();
        return String.join(FIELD,
                shard.nodeId(),
                e.httpHost(),
                Integer.toString(e.httpPort()),
                Integer.toString(e.grpcPort()),
                Integer.toString(e.binaryIngressPort()),
                shard.state().name(),
                Long.toString(shard.generation()),
                Integer.toString(shard.symbolId()),
                Long.toString(shard.updatedAtEpochMillis())
        );
    }

    public static RegisteredShard decodeShard(String shardKey, String payload) {
        Objects.requireNonNull(shardKey, "shardKey");
        Objects.requireNonNull(payload, "payload");
        String[] parts = payload.split("\\|", -1);
        if (parts.length != 9) {
            throw new IllegalStateException("invalid shard orchestrator payload for shardKey=" + shardKey);
        }
        return new RegisteredShard(
                shardKey,
                Integer.parseInt(parts[7]),
                parts[0],
                new ShardEndpoints(
                        parts[1],
                        Integer.parseInt(parts[2]),
                        Integer.parseInt(parts[3]),
                        Integer.parseInt(parts[4])
                ),
                ShardLifecycleState.valueOf(parts[5]),
                Long.parseLong(parts[6]),
                Long.parseLong(parts[8])
        );
    }

    public static String encodeSymbolRoute(int symbolId, String shardKey, long generation, long updatedAtEpochMillis) {
        if (symbolId <= 0) {
            throw new IllegalArgumentException("symbolId must be positive");
        }
        return String.join(FIELD,
                shardKey,
                Long.toString(generation),
                Long.toString(updatedAtEpochMillis)
        );
    }

    public static SymbolRouteHeader decodeSymbolRoute(int symbolId, String payload) {
        Objects.requireNonNull(payload, "payload");
        String[] parts = payload.split("\\|", -1);
        if (parts.length != 3) {
            throw new IllegalStateException("invalid symbol route payload for symbolId=" + symbolId);
        }
        return new SymbolRouteHeader(
                symbolId,
                parts[0],
                Long.parseLong(parts[1]),
                Long.parseLong(parts[2])
        );
    }

    public record SymbolRouteHeader(int symbolId, String shardKey, long generation, long updatedAtEpochMillis) {
    }
}
