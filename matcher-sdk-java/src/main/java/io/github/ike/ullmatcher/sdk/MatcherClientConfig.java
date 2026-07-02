package io.github.ike.ullmatcher.sdk;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

public record MatcherClientConfig(URI endpoint, Duration requestTimeout) {
    public MatcherClientConfig {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
    }

    public static MatcherClientConfig localDefault() {
        return new MatcherClientConfig(URI.create("http://127.0.0.1:8080"), Duration.ofSeconds(2));
    }
}
