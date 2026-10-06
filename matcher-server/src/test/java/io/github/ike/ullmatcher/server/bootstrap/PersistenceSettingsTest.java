package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PersistenceSettingsTest {
    @Test
    void prodServerModeExpandsProdProfile() throws Exception {
        Path dir = Files.createTempDirectory("persistence-prod");
        MatcherServerConfig defaults = MatcherServerConfig.defaults("node-a", 1, dir);
        MatcherServerConfig.Builder builder = defaults.toBuilder().serverMode(MatcherServerMode.PROD);
        PersistenceSettings.apply(builder, MatcherServerMode.PROD, defaults);
        MatcherServerConfig built = builder.build();

        assertEquals(PersistenceProfile.PROD, built.persistenceProfile());
        assertEquals(WalDurabilityMode.SYNC_PER_COMMAND, built.walDurabilityMode());
        assertEquals(PersistenceProfile.PROD_SNAPSHOT_INTERVAL_MILLIS, built.snapshotIntervalMillis());
    }

    @Test
    void benchProfileUsesBatchedFsync() throws Exception {
        Path dir = Files.createTempDirectory("persistence-bench");
        MatcherServerConfig defaults = MatcherServerConfig.defaults("node-a", 1, dir);
        MatcherServerConfig.Builder builder = defaults.toBuilder().serverMode(MatcherServerMode.DEV);
        PersistenceSettings.apply(builder, MatcherServerMode.DEV, defaults, PersistenceProfile.BENCH);
        MatcherServerConfig built = builder.build();

        assertEquals(PersistenceProfile.BENCH, built.persistenceProfile());
        assertEquals(WalDurabilityMode.SYNC_PER_BATCH, built.walDurabilityMode());
        assertEquals(PersistenceProfile.BENCH_WAL_FORCE_BATCH_SIZE, built.walForceBatchSize());
        assertEquals(PersistenceProfile.BENCH_WAL_FORCE_MAX_DELAY_MICROS, built.walForceMaxDelayMicros());
        assertEquals(0L, built.snapshotIntervalMillis());
    }

    @Test
    void springOverridesReplaceProfilePresets() throws Exception {
        Path dir = Files.createTempDirectory("persistence-spring");
        MatcherServerConfig defaults = MatcherServerConfig.defaults("node-a", 1, dir);
        MatcherServerConfig.Builder builder = defaults.toBuilder().serverMode(MatcherServerMode.DEV);
        PersistenceSettings.apply(
                builder,
                MatcherServerMode.DEV,
                defaults,
                PersistenceProfile.BENCH,
                new PersistenceSettings.PropertyOverrides(
                        WalDurabilityMode.SYNC_PER_COMMAND,
                        8,
                        250L,
                        30_000L));
        MatcherServerConfig built = builder.build();

        assertEquals(PersistenceProfile.BENCH, built.persistenceProfile());
        assertEquals(WalDurabilityMode.SYNC_PER_COMMAND, built.walDurabilityMode());
        assertEquals(8, built.walForceBatchSize());
        assertEquals(250L, built.walForceMaxDelayMicros());
        assertEquals(30_000L, built.snapshotIntervalMillis());
    }

    @Test
    void invalidProfilePropertyFailsFast() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> PersistenceSettings.parseProfile("typo"));
        assertEquals(
                "invalid matcher.persistenceProfile=typo; expected PROD, BENCH, LAB, or NONE",
                error.getMessage());
    }

    @Test
    void invalidWalDurabilityPropertyFailsFast() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> PersistenceSettings.parseWalDurability("typo"));
        assertTrue(error.getMessage().contains("matcher.walDurabilityMode=typo"));
    }
}
