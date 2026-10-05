/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import io.undertow.server.HttpServerExchange;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global, route and endpoint admission. Acquire and release are paired so a rejected
 * request never leaks a permit.
 */
final class HttpBudgetGuard {
    private final ThreadPoolExecutor requestExecutor;
    private final int requestExecutorQueueCapacity;
    private final Semaphore requestSlots;
    private final int maxConcurrentRequests;
    private final AtomicLong globalOverloadCount;
    private final HttpJsonCodec json;

    HttpBudgetGuard(ThreadPoolExecutor requestExecutor,
                    int requestExecutorQueueCapacity,
                    Semaphore requestSlots,
                    int maxConcurrentRequests,
                    AtomicLong globalOverloadCount,
                    HttpJsonCodec json) {
        this.requestExecutor = Objects.requireNonNull(requestExecutor, "requestExecutor");
        this.requestExecutorQueueCapacity = requestExecutorQueueCapacity;
        this.requestSlots = Objects.requireNonNull(requestSlots, "requestSlots");
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.globalOverloadCount = Objects.requireNonNull(globalOverloadCount, "globalOverloadCount");
        this.json = Objects.requireNonNull(json, "json");
    }

    boolean tryAcquire(HttpServerExchange exchange,
                       String operation,
                       RouteBudget routeBudget,
                       HttpEndpointBudget endpointBudget,
                       EndpointStats endpoint,
                       boolean requireExecutorCapacity) {
        if (requireExecutorCapacity
                && requestExecutor.getActiveCount() >= requestExecutor.getMaximumPoolSize()
                && requestExecutor.getQueue().remainingCapacity() == 0) {
            globalOverloadCount.incrementAndGet();
            routeBudget.overloadCount().incrementAndGet();
            endpoint.overloadCount().incrementAndGet();
            json.writeBestEffort(exchange, new OverloadedException(
                    "http request executor is saturated; workers=" + requestExecutor.getMaximumPoolSize()
                            + " queueCapacity=" + requestExecutorQueueCapacity
            ));
            return false;
        }
        if (!requestSlots.tryAcquire()) {
            globalOverloadCount.incrementAndGet();
            routeBudget.overloadCount().incrementAndGet();
            endpoint.overloadCount().incrementAndGet();
            json.writeBestEffort(exchange, new OverloadedException(
                    "too many in-flight requests; limit=" + maxConcurrentRequests
            ));
            return false;
        }
        if (!routeBudget.slots().tryAcquire()) {
            routeBudget.overloadCount().incrementAndGet();
            json.writeBestEffort(exchange, new OverloadedException(
                    operation + " route budget exhausted; limit=" + routeBudget.maxConcurrentRequests()
            ));
            requestSlots.release();
            return false;
        }
        if (endpointBudget != null && !endpointBudget.slots().tryAcquire()) {
            endpoint.overloadCount().incrementAndGet();
            json.writeBestEffort(exchange, new OverloadedException(
                    operation + " endpoint budget exhausted; limit=" + endpointBudget.maxConcurrentRequests()
            ));
            routeBudget.slots().release();
            requestSlots.release();
            return false;
        }
        return true;
    }

    void release(RouteBudget routeBudget, HttpEndpointBudget endpointBudget) {
        if (endpointBudget != null) {
            endpointBudget.slots().release();
        }
        routeBudget.slots().release();
        requestSlots.release();
    }

    int availablePermits() {
        return requestSlots.availablePermits();
    }

    long globalOverloadCount() {
        return globalOverloadCount.get();
    }

    int executorQueueDepth() {
        return requestExecutor.getQueue().size();
    }

    int executorQueueCapacity() {
        return requestExecutorQueueCapacity;
    }

    int maxConcurrentRequests() {
        return maxConcurrentRequests;
    }
}
