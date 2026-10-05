package io.github.ike.ullmatcher.sdk;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class MatcherClientConfigTest {
    @Test
    void localDefaultTargetsLoopbackWithoutApiKey() {
        MatcherClientConfig config = MatcherClientConfig.localDefault();

        assertEquals(URI.create("http://127.0.0.1:8080"), config.endpoint());
        assertEquals(Duration.ofSeconds(2), config.requestTimeout());
        assertNull(config.ingressApiKey());
        assertEquals(MatcherClientConfig.DEFAULT_API_KEY_HEADER, config.ingressApiKeyHeader());
    }

    @Test
    void apiKeyOnlyConstructorKeepsDefaultHeader() {
        MatcherClientConfig config = new MatcherClientConfig(
                URI.create("http://127.0.0.1:9090"),
                Duration.ofMillis(250),
                "secret"
        );

        assertEquals("secret", config.ingressApiKey());
        assertEquals("X-Ull-Api-Key", config.ingressApiKeyHeader());
    }

    @Test
    void blankOrMissingHeaderFallsBackToDefaultHeader() {
        URI endpoint = URI.create("http://127.0.0.1:9090");

        assertEquals(MatcherClientConfig.DEFAULT_API_KEY_HEADER,
                new MatcherClientConfig(endpoint, Duration.ofSeconds(1), "secret", null).ingressApiKeyHeader());
        assertEquals(MatcherClientConfig.DEFAULT_API_KEY_HEADER,
                new MatcherClientConfig(endpoint, Duration.ofSeconds(1), "secret", "   ").ingressApiKeyHeader());
    }

    @Test
    void explicitHeaderIsPreserved() {
        MatcherClientConfig config = new MatcherClientConfig(
                URI.create("http://127.0.0.1:9090"),
                Duration.ofSeconds(1),
                "secret",
                "X-Tenant-Key"
        );

        assertEquals("X-Tenant-Key", config.ingressApiKeyHeader());
    }

    @Test
    void rejectsNonPositiveRequestTimeout() {
        URI endpoint = URI.create("http://127.0.0.1:9090");

        assertThrows(IllegalArgumentException.class, () -> new MatcherClientConfig(endpoint, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new MatcherClientConfig(endpoint, Duration.ofMillis(-1)));
    }

    @Test
    void rejectsBlankApiKeyAndMissingRequiredValues() {
        URI endpoint = URI.create("http://127.0.0.1:9090");

        assertThrows(IllegalArgumentException.class, () -> new MatcherClientConfig(endpoint, Duration.ofSeconds(1), " "));
        assertThrows(NullPointerException.class, () -> new MatcherClientConfig(null, Duration.ofSeconds(1)));
        assertThrows(NullPointerException.class, () -> new MatcherClientConfig(endpoint, null));
    }
}
