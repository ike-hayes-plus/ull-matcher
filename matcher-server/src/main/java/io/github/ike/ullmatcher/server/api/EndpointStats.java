package io.github.ike.ullmatcher.server.api;

import java.util.concurrent.atomic.AtomicLong;

record EndpointStats(
        String endpointName,
        String routeName,
        int routeMaxConcurrentRequests,
        int endpointMaxConcurrentRequests,
        AtomicLong requestCount,
        AtomicLong overloadCount,
        AtomicLong timeoutCount,
        AtomicLong failureCount,
        AtomicLong inflight,
        AtomicLong maxInflight,
        AtomicLong durationSumMillis,
        AtomicLong durationMaxMillis,
        AtomicLong[] bucketCounts
) {
    static EndpointStats create(String endpointName, String routeName, int routeMaxConcurrentRequests, int endpointMaxConcurrentRequests) {
        return new EndpointStats(
                endpointName,
                routeName,
                routeMaxConcurrentRequests,
                endpointMaxConcurrentRequests,
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong(),
                newAtomicLongArray(HttpRouteMetrics.ENDPOINT_LATENCY_BUCKETS_MILLIS.length)
        );
    }

    private static AtomicLong[] newAtomicLongArray(int size) {
        AtomicLong[] values = new AtomicLong[size];
        for (int i = 0; i < size; i++) {
            values[i] = new AtomicLong();
        }
        return values;
    }
}
