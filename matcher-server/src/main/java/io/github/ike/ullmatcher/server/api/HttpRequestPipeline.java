/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Applies budget, auth and dispatch around a blocking handler.
 */
final class HttpRequestPipeline {
    @FunctionalInterface
    interface BlockingExchangeHandler {
        void handle(HttpServerExchange exchange) throws IOException;
    }

    private final HttpBudgetGuard budgets;
    private final HttpAuthFilter auth;
    private final HttpJsonCodec json;
    private final HttpDispatchExecutor dispatchExecutor;
    private final Map<String, EndpointStats> endpointStats;

    HttpRequestPipeline(HttpBudgetGuard budgets,
                        HttpAuthFilter auth,
                        HttpJsonCodec json,
                        HttpDispatchExecutor dispatchExecutor,
                        Map<String, EndpointStats> endpointStats) {
        this.budgets = Objects.requireNonNull(budgets, "budgets");
        this.auth = Objects.requireNonNull(auth, "auth");
        this.json = Objects.requireNonNull(json, "json");
        this.dispatchExecutor = Objects.requireNonNull(dispatchExecutor, "dispatchExecutor");
        this.endpointStats = Objects.requireNonNull(endpointStats, "endpointStats");
    }

    HttpHandler blocking(String endpointMetricKey,
                         String operation,
                         RouteBudget routeBudget,
                         HttpEndpointBudget endpointBudget,
                         boolean requireIngressAuth,
                         BlockingExchangeHandler handler) {
        return exchange -> {
            EndpointStats endpoint = endpointFor(endpointMetricKey, routeBudget, endpointBudget);
            if (!budgets.tryAcquire(
                    exchange,
                    operation,
                    routeBudget,
                    endpointBudget,
                    endpoint,
                    true)) {
                return;
            }
            if (auth.rejectUnauthorized(exchange, requireIngressAuth)) {
                budgets.release(routeBudget, endpointBudget);
                return;
            }
            try {
                exchange.dispatch(dispatchExecutor.executor(), () -> runOnDispatchThread(
                        exchange,
                        operation,
                        routeBudget,
                        endpointBudget,
                        endpoint,
                        handler));
            } catch (RejectedExecutionException e) {
                budgets.release(routeBudget, endpointBudget);
                json.writeBestEffort(exchange, new OverloadedException("http dispatch executor rejected work"));
            }
        };
    }

    private void runOnDispatchThread(HttpServerExchange exchange,
                                     String operation,
                                     RouteBudget routeBudget,
                                     HttpEndpointBudget endpointBudget,
                                     EndpointStats endpoint,
                                     BlockingExchangeHandler handler) {
        json.responseGuard(exchange);
        long startedAt = System.nanoTime();
        long endpointInflight = endpoint.inflight().incrementAndGet();
        endpoint.maxInflight().accumulateAndGet(endpointInflight, Math::max);
        try {
            routeBudget.requestCount().incrementAndGet();
            endpoint.requestCount().incrementAndGet();
            HttpRouteTimeoutGuard.run(
                    exchange,
                    routeBudget.timeoutMillis(),
                    operation,
                    json,
                    routeBudget,
                    endpoint,
                    () -> handler.handle(exchange));
        } catch (IOException e) {
            endpoint.failureCount().incrementAndGet();
            json.writeBestEffort(exchange, e);
        } catch (RuntimeException e) {
            endpoint.failureCount().incrementAndGet();
            json.writeBestEffort(exchange, e);
        } finally {
            recordEndpointLatency(endpoint, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
            endpoint.inflight().decrementAndGet();
            budgets.release(routeBudget, endpointBudget);
        }
    }

    private EndpointStats endpointFor(String endpointMetricKey, RouteBudget routeBudget, HttpEndpointBudget endpointBudget) {
        return endpointStats.computeIfAbsent(
                endpointMetricKey,
                ignored -> EndpointStats.create(
                        endpointMetricKey,
                        routeBudget.name(),
                        routeBudget.maxConcurrentRequests(),
                        endpointBudget == null ? 0 : endpointBudget.maxConcurrentRequests())
        );
    }

    private static void recordEndpointLatency(EndpointStats endpoint, long elapsedMillis) {
        endpoint.durationSumMillis().addAndGet(elapsedMillis);
        endpoint.durationMaxMillis().accumulateAndGet(elapsedMillis, Math::max);
        long[] buckets = HttpRouteMetrics.ENDPOINT_LATENCY_BUCKETS_MILLIS;
        for (int i = 0; i < buckets.length; i++) {
            if (elapsedMillis <= buckets[i]) {
                endpoint.bucketCounts()[i].incrementAndGet();
            }
        }
    }

}
