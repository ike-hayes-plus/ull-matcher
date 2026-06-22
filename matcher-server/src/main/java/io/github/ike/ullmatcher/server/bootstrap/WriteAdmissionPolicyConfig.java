package io.github.ike.ullmatcher.server.bootstrap;

import java.util.Objects;

public record WriteAdmissionPolicyConfig(
        int shardMaxConcurrentRequests,
        int tenantMaxConcurrentRequests,
        String tenantAdmissionHeader,
        double shardRateLimitPerSecond,
        int shardRateBurst,
        double tenantRateLimitPerSecond,
        int tenantRateBurst,
        int tenantDefaultWeight,
        String tenantWeightOverrides,
        String tenantPriorityHeader
) {
    public WriteAdmissionPolicyConfig {
        Objects.requireNonNull(tenantAdmissionHeader, "tenantAdmissionHeader");
        Objects.requireNonNull(tenantWeightOverrides, "tenantWeightOverrides");
        Objects.requireNonNull(tenantPriorityHeader, "tenantPriorityHeader");
        if (shardMaxConcurrentRequests < 0 || tenantMaxConcurrentRequests < 0
                || shardRateLimitPerSecond < 0.0d || shardRateBurst < 0
                || tenantRateLimitPerSecond < 0.0d || tenantRateBurst < 0
                || tenantDefaultWeight <= 0
                || tenantAdmissionHeader.isBlank() || tenantPriorityHeader.isBlank()) {
            throw new IllegalArgumentException("invalid write admission policy configuration");
        }
    }

    public static WriteAdmissionPolicyConfig defaults() {
        return new WriteAdmissionPolicyConfig(
                128,
                0,
                "X-Ull-Tenant-Key",
                0.0d,
                0,
                0.0d,
                0,
                1,
                "",
                "X-Ull-Tenant-Priority"
        );
    }
}
