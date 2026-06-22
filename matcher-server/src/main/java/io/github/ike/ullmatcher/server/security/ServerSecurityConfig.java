package io.github.ike.ullmatcher.server.security;

import io.github.ike.ullmatcher.ha.grpc.security.GrpcClientTlsConfig;
import io.github.ike.ullmatcher.ha.grpc.security.GrpcServerTlsConfig;

import java.nio.file.Path;

public record ServerSecurityConfig(
        TransportSecurityConfig transportSecurity,
        GrpcServerTlsConfig grpcServerTls,
        GrpcClientTlsConfig grpcClientTls,
        long tlsReloadIntervalMillis,
        boolean openTelemetryMetricsEnabled
) {
    public ServerSecurityConfig {
        if (tlsReloadIntervalMillis < 0L) {
            throw new IllegalArgumentException("tlsReloadIntervalMillis must be non-negative");
        }
    }

    public static ServerSecurityConfig insecureDefaults() {
        return new ServerSecurityConfig(null, null, GrpcClientTlsConfig.insecure(), 0L, false);
    }

    public boolean tlsReloadEnabled() {
        return transportSecurity != null && tlsReloadIntervalMillis > 0L;
    }

    public boolean transportSecurityEnabled() {
        return transportSecurity != null;
    }

    public static ServerSecurityConfig fromPaths(Path certChain,
                                                 Path privateKey,
                                                 Path trustChain,
                                                 boolean requireMtls,
                                                 long reloadIntervalMillis,
                                                 boolean enableOpenTelemetryMetrics) {
        TransportSecurityConfig transportSecurity = certChain == null || privateKey == null
                ? null
                : new TransportSecurityConfig(certChain, privateKey, trustChain, requireMtls);
        GrpcServerTlsConfig serverTls = transportSecurity == null
                ? null
                : new GrpcServerTlsConfig(
                transportSecurity.certificateChainFile(),
                transportSecurity.privateKeyFile(),
                transportSecurity.trustCertCollectionFile(),
                transportSecurity.requireMutualTls()
        );
        GrpcClientTlsConfig clientTls = serverTls == null
                ? GrpcClientTlsConfig.insecure()
                : new GrpcClientTlsConfig(false, trustChain, certChain, privateKey, "");
        return new ServerSecurityConfig(transportSecurity, serverTls, clientTls, reloadIntervalMillis, enableOpenTelemetryMetrics);
    }
}
