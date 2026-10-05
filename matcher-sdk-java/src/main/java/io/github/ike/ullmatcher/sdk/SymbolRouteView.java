package io.github.ike.ullmatcher.sdk;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

/**
 * {@code symbolId} 经编排面解析后的分片 HTTP/gRPC 端点视图。
 */
public record SymbolRouteView(
        int symbolId,
        String shardKey,
        long generation,
        long updatedAtEpochMillis,
        boolean active,
        String httpHost,
        int httpPort,
        int grpcPort,
        Integer binaryIngressPort,
        String nodeId,
        String state
) {
    public SymbolRouteView {
        if (symbolId <= 0) {
            throw new IllegalArgumentException("symbolId must be positive");
        }
        Objects.requireNonNull(shardKey, "shardKey");
        if (shardKey.isBlank()) {
            throw new IllegalArgumentException("shardKey must not be blank");
        }
        Objects.requireNonNull(httpHost, "httpHost");
        if (httpHost.isBlank()) {
            throw new IllegalArgumentException("httpHost must not be blank");
        }
        if (httpPort <= 0 || grpcPort <= 0) {
            throw new IllegalArgumentException("httpPort and grpcPort must be positive");
        }
        if (generation < 0L || updatedAtEpochMillis < 0L) {
            throw new IllegalArgumentException("generation and updatedAtEpochMillis must be non-negative");
        }
    }

    public MatcherClientConfig toMatcherClientConfig(Duration requestTimeout) {
        return toMatcherClientConfig(requestTimeout, null, MatcherClientConfig.DEFAULT_API_KEY_HEADER);
    }

    public MatcherClientConfig toMatcherClientConfig(Duration requestTimeout, String ingressApiKey) {
        return toMatcherClientConfig(requestTimeout, ingressApiKey, MatcherClientConfig.DEFAULT_API_KEY_HEADER);
    }

    public MatcherClientConfig toMatcherClientConfig(
            Duration requestTimeout,
            String ingressApiKey,
            String ingressApiKeyHeader) {
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        return new MatcherClientConfig(shardHttpUri(), requestTimeout, ingressApiKey, ingressApiKeyHeader);
    }

    public URI shardHttpUri() {
        if (httpHost.indexOf(':') >= 0 && !httpHost.startsWith("[")) {
            return URI.create("http://[" + httpHost + "]:" + httpPort);
        }
        return URI.create("http://" + httpHost + ":" + httpPort);
    }
}
