package io.github.ike.ullmatcher.ha.etcd;

import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;

/**
 * etcd control-plane configuration.
 *
 * @param endpoint etcd HTTP(S) endpoints, comma-separated
 * @param keyPrefix key prefix used by this matcher deployment
 * @param leaseTtlSeconds etcd lease TTL in seconds
 * @param timeoutMillis HTTP request timeout in milliseconds
 * @param trustChainFile optional PEM trust bundle for private CAs
 * @param certificateChainFile optional client certificate chain for mTLS
 * @param privateKeyFile optional client private key for mTLS
 * @param enforceProductionSafety when true, production transport rules are enforced at construction time
 */
public record EtcdConfig(
        String endpoint,
        String keyPrefix,
        long leaseTtlSeconds,
        long timeoutMillis,
        Path trustChainFile,
        Path certificateChainFile,
        Path privateKeyFile,
        boolean enforceProductionSafety
) {
    public EtcdConfig {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        if (endpoint.isBlank() || keyPrefix.isBlank() || !keyPrefix.startsWith("/")) {
            throw new IllegalArgumentException("endpoint must not be blank and keyPrefix must be an absolute path");
        }
        if (leaseTtlSeconds <= 0L || timeoutMillis <= 0L) {
            throw new IllegalArgumentException("leaseTtlSeconds and timeoutMillis must be positive");
        }
        if (certificateChainFile == null ^ privateKeyFile == null) {
            throw new IllegalArgumentException("etcd mTLS requires both certificateChainFile and privateKeyFile, or neither");
        }
        if (enforceProductionSafety) {
            requireSecureEndpoints(endpoint);
        }
    }

    public static EtcdConfig defaults(String endpoint, String clusterName) {
        return new EtcdConfig(endpoint, "/ull-matcher/" + clusterName, 10L, 2_000L, null, null, null, false);
    }

    /**
     * Returns a copy of this configuration with production transport rules enforced.
     *
     * @return production-validated configuration
     */
    public EtcdConfig withProductionSafety() {
        return enforceProductionSafety ? this : new EtcdConfig(
                endpoint, keyPrefix, leaseTtlSeconds, timeoutMillis,
                trustChainFile, certificateChainFile, privateKeyFile, true);
    }

    public void validateProductionSafety() {
        requireSecureEndpoints(endpoint);
    }

    private static void requireSecureEndpoints(String rawEndpoints) {
        for (URI uri : parseEndpointUris(rawEndpoints)) {
            String scheme = uri.getScheme() == null ? "http" : uri.getScheme().toLowerCase(java.util.Locale.ROOT);
            String host = uri.getHost() == null ? "" : uri.getHost();
            if ("http".equals(scheme) && !isLoopbackHost(host)) {
                throw new IllegalStateException("prod mode requires https etcd endpoints for non-loopback hosts: " + uri);
            }
        }
    }

    static boolean isLoopbackHost(String host) {
        return "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host) || "::1".equals(host);
    }

    static java.util.List<URI> parseEndpointUris(String rawEndpoints) {
        java.util.ArrayList<URI> parsed = new java.util.ArrayList<>();
        for (String token : rawEndpoints.split(",")) {
            String endpoint = token.trim();
            if (!endpoint.isEmpty()) {
                parsed.add(URI.create(endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint));
            }
        }
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException("at least one etcd endpoint is required");
        }
        return java.util.List.copyOf(parsed);
    }
}
