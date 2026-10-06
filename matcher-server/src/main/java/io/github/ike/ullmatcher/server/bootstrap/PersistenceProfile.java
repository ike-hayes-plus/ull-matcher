package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.hft.WalDurabilityMode;

import java.util.concurrent.TimeUnit;

/**
 * Redis-style RDB + AOF presets: snapshot cadence plus WAL durability without changing on-disk formats.
 */
public enum PersistenceProfile {
    /** 未点名预设：沿用 seed/DEV WAL 默认；{@link MatcherServerMode#PROD} 仍解析为 {@link #PROD}。 */
    NONE,

    /** Production: fsync per command, periodic RDB, cold WAL archive required at validation. */
    PROD,

    /** Throughput benchmarks: batched fsync, no periodic snapshot unless overridden. */
    BENCH,

    /** Local experiments: OS-buffered WAL; forbidden in {@link MatcherServerMode#PROD}. */
    LAB;

    public static final long PROD_SNAPSHOT_INTERVAL_MILLIS = 60_000L;
    public static final int BENCH_WAL_FORCE_BATCH_SIZE = 32;
    public static final long BENCH_WAL_FORCE_MAX_DELAY_MICROS = TimeUnit.SECONDS.toMicros(1);

    public WalDurabilityMode walDurabilityMode() {
        return switch (this) {
            case NONE, PROD -> WalDurabilityMode.SYNC_PER_COMMAND;
            case BENCH -> WalDurabilityMode.SYNC_PER_BATCH;
            case LAB -> WalDurabilityMode.OS_BUFFERED;
        };
    }

    public int walForceBatchSize() {
        return switch (this) {
            case BENCH -> BENCH_WAL_FORCE_BATCH_SIZE;
            default -> MatcherServerConfig.DEFAULT_WAL_FORCE_BATCH_SIZE;
        };
    }

    public long walForceMaxDelayMicros() {
        return switch (this) {
            case BENCH -> BENCH_WAL_FORCE_MAX_DELAY_MICROS;
            default -> MatcherServerConfig.DEFAULT_WAL_FORCE_MAX_DELAY_MICROS;
        };
    }

    public long snapshotIntervalMillis() {
        return this == PROD ? PROD_SNAPSHOT_INTERVAL_MILLIS : 0L;
    }
}
