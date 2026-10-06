package io.github.ike.ullmatcher.server.bootstrap;

import io.github.ike.ullmatcher.hft.WalDurabilityMode;

/**
 * Resolves {@link PersistenceProfile} and applies WAL / snapshot presets with optional JVM or Spring overrides.
 */
public final class PersistenceSettings {
    public static final String PROFILE_PROPERTY = "matcher.persistenceProfile";
    public static final String SNAPSHOT_INTERVAL_PROPERTY = "matcher.snapshotIntervalMillis";
    public static final String WAL_DURABILITY_PROPERTY = "matcher.walDurabilityMode";
    public static final String WAL_FORCE_BATCH_PROPERTY = "matcher.walForceBatchSize";
    public static final String WAL_FORCE_DELAY_PROPERTY = "matcher.walForceMaxDelayMicros";

    /**
     * Explicit Spring / YAML overrides. {@code null} fields leave the resolved profile preset unchanged.
     */
    public record PropertyOverrides(
            WalDurabilityMode walDurabilityMode,
            Integer walForceBatchSize,
            Long walForceMaxDelayMicros,
            Long snapshotIntervalMillis
    ) {
        static PropertyOverrides fromSystemProperties() {
            return new PropertyOverrides(
                    present(WAL_DURABILITY_PROPERTY) ? parseWalDurability(System.getProperty(WAL_DURABILITY_PROPERTY)) : null,
                    present(WAL_FORCE_BATCH_PROPERTY) ? Integer.parseInt(System.getProperty(WAL_FORCE_BATCH_PROPERTY).trim()) : null,
                    present(WAL_FORCE_DELAY_PROPERTY) ? Long.parseLong(System.getProperty(WAL_FORCE_DELAY_PROPERTY).trim()) : null,
                    present(SNAPSHOT_INTERVAL_PROPERTY) ? Long.parseLong(System.getProperty(SNAPSHOT_INTERVAL_PROPERTY).trim()) : null
            );
        }

        private static boolean present(String property) {
            return System.getProperty(property) != null;
        }
    }

    private PersistenceSettings() {}

    public static void apply(MatcherServerConfig.Builder builder,
                             MatcherServerMode serverMode,
                             MatcherServerConfig seedDefaults) {
        apply(builder, serverMode, seedDefaults, configuredProfileFromProperty());
    }

    public static void apply(MatcherServerConfig.Builder builder,
                             MatcherServerMode serverMode,
                             MatcherServerConfig seedDefaults,
                             PersistenceProfile configuredProfile) {
        applyPresets(builder, serverMode, seedDefaults, configuredProfile);
        applyPropertyOverrides(builder, PropertyOverrides.fromSystemProperties());
    }

    public static void apply(MatcherServerConfig.Builder builder,
                             MatcherServerMode serverMode,
                             MatcherServerConfig seedDefaults,
                             PersistenceProfile configuredProfile,
                             PropertyOverrides springOverrides) {
        applyPresets(builder, serverMode, seedDefaults, configuredProfile);
        applyPropertyOverrides(builder, springOverrides);
    }

    private static void applyPresets(MatcherServerConfig.Builder builder,
                                     MatcherServerMode serverMode,
                                     MatcherServerConfig seedDefaults,
                                     PersistenceProfile configuredProfile) {
        PersistenceProfile profile = resolve(serverMode, configuredProfile);
        builder.persistenceProfile(profile);

        WalDurabilityMode durability;
        int batchSize;
        long maxDelayMicros;
        long snapshotIntervalMillis;
        if (profile == PersistenceProfile.NONE) {
            durability = seedDefaults.walDurabilityMode();
            batchSize = seedDefaults.walForceBatchSize();
            maxDelayMicros = seedDefaults.walForceMaxDelayMicros();
            snapshotIntervalMillis = seedDefaults.snapshotIntervalMillis();
        } else {
            durability = profile.walDurabilityMode();
            batchSize = profile.walForceBatchSize();
            maxDelayMicros = profile.walForceMaxDelayMicros();
            snapshotIntervalMillis = profile.snapshotIntervalMillis();
        }

        builder.walDurabilityMode(durability)
                .walForceBatchSize(batchSize)
                .walForceMaxDelayMicros(maxDelayMicros)
                .snapshotIntervalMillis(snapshotIntervalMillis);
    }

    private static void applyPropertyOverrides(MatcherServerConfig.Builder builder, PropertyOverrides overrides) {
        if (overrides == null) {
            return;
        }
        if (overrides.walDurabilityMode() != null) {
            builder.walDurabilityMode(overrides.walDurabilityMode());
        }
        if (overrides.walForceBatchSize() != null) {
            builder.walForceBatchSize(overrides.walForceBatchSize());
        }
        if (overrides.walForceMaxDelayMicros() != null) {
            builder.walForceMaxDelayMicros(overrides.walForceMaxDelayMicros());
        }
        if (overrides.snapshotIntervalMillis() != null) {
            builder.snapshotIntervalMillis(overrides.snapshotIntervalMillis());
        }
    }

    public static PersistenceProfile resolve(MatcherServerMode serverMode, PersistenceProfile configuredProfile) {
        if (configuredProfile != null && configuredProfile != PersistenceProfile.NONE) {
            return configuredProfile;
        }
        String property = System.getProperty(PROFILE_PROPERTY);
        if (property != null && !property.isBlank()) {
            return parseProfile(property);
        }
        return serverMode == MatcherServerMode.PROD ? PersistenceProfile.PROD : PersistenceProfile.NONE;
    }

    private static PersistenceProfile configuredProfileFromProperty() {
        String property = System.getProperty(PROFILE_PROPERTY);
        if (property == null || property.isBlank()) {
            return PersistenceProfile.NONE;
        }
        return parseProfile(property);
    }

    static PersistenceProfile parseProfile(String raw) {
        try {
            return PersistenceProfile.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid " + PROFILE_PROPERTY + "=" + raw + "; expected PROD, BENCH, LAB, or NONE", e);
        }
    }

    static WalDurabilityMode parseWalDurability(String raw) {
        try {
            return WalDurabilityMode.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "invalid " + WAL_DURABILITY_PROPERTY + "=" + raw + "; expected a WalDurabilityMode enum name", e);
        }
    }
}
