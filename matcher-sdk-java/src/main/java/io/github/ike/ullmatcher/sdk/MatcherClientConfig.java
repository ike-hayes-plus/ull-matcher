package io.github.ike.ullmatcher.sdk;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

/**
 * Client configuration for the matcher HTTP and binary APIs.
 *
 * @param endpoint base endpoint of the matcher HTTP API
 * @param requestTimeout per-request timeout
 * @param ingressApiKey optional shared secret presented to the ingress, or null when auth is disabled
 * @param ingressApiKeyHeader header carrying the api key; must match the server's configured header
 */
public record MatcherClientConfig(URI endpoint, Duration requestTimeout, String ingressApiKey, String ingressApiKeyHeader) {
    public static final String DEFAULT_API_KEY_HEADER = "X-Ull-Api-Key";

    public MatcherClientConfig {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (ingressApiKey != null && ingressApiKey.isBlank()) {
            throw new IllegalArgumentException("ingressApiKey must not be blank when provided");
        }
        if (ingressApiKeyHeader == null || ingressApiKeyHeader.isBlank()) {
            ingressApiKeyHeader = DEFAULT_API_KEY_HEADER;
        }
    }

    public MatcherClientConfig(URI endpoint, Duration requestTimeout) {
        this(endpoint, requestTimeout, null, DEFAULT_API_KEY_HEADER);
    }

    public MatcherClientConfig(URI endpoint, Duration requestTimeout, String ingressApiKey) {
        this(endpoint, requestTimeout, ingressApiKey, DEFAULT_API_KEY_HEADER);
    }

    public static MatcherClientConfig localDefault() {
        return new MatcherClientConfig(URI.create("http://127.0.0.1:8080"), Duration.ofSeconds(2));
    }
}
