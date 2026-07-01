package io.github.ike.ullmatcher.server.api;

final class HttpRouteMetrics {
    static final long[] ENDPOINT_LATENCY_BUCKETS_MILLIS = {10L, 50L, 100L, 250L, 500L, 1_000L, 2_500L, 5_000L};

    private HttpRouteMetrics() {}
}
