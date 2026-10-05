package io.github.ike.ullmatcher.server.orchestrator;

import io.github.ike.ullmatcher.ha.etcd.EtcdConfig;

import java.util.Objects;

/**
 * 3.0 分片在控制面编排存储中的自注册配置（默认关闭，2.0 行为不变）。
 */
public record OrchestratorRegistrationConfig(
        boolean enabled,
        long generation,
        EtcdConfig etcdConfig
) {
    public OrchestratorRegistrationConfig {
        if (generation < 0L) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        if (enabled && etcdConfig == null) {
            throw new IllegalArgumentException("etcdConfig is required when orchestrator registration is enabled");
        }
    }

    public static OrchestratorRegistrationConfig disabled() {
        return new OrchestratorRegistrationConfig(false, 1L, null);
    }

    public static OrchestratorRegistrationConfig etcd(long generation, EtcdConfig etcdConfig) {
        Objects.requireNonNull(etcdConfig, "etcdConfig");
        return new OrchestratorRegistrationConfig(true, generation, etcdConfig);
    }
}
