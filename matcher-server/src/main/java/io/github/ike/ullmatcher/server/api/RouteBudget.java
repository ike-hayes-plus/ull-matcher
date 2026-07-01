package io.github.ike.ullmatcher.server.api;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

record RouteBudget(
        String name,
        int maxConcurrentRequests,
        Semaphore slots,
        long timeoutMillis,
        AtomicLong requestCount,
        AtomicLong overloadCount,
        AtomicLong timeoutCount
) {
    static RouteBudget create(String name, int maxConcurrentRequests, long timeoutMillis) {
        return new RouteBudget(
                name,
                maxConcurrentRequests,
                new Semaphore(maxConcurrentRequests),
                timeoutMillis,
                new AtomicLong(),
                new AtomicLong(),
                new AtomicLong()
        );
    }
}
