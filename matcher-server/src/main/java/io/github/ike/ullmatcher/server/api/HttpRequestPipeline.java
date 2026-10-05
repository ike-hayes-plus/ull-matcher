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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    private final HttpReadRequestExecutor readRequestExecutor;
    private final Map<String, EndpointStats> endpointStats;

    HttpRequestPipeline(HttpBudgetGuard budgets,
                        HttpAuthFilter auth,
                        HttpJsonCodec json,
                        HttpReadRequestExecutor readRequestExecutor,
                        Map<String, EndpointStats> endpointStats) {
        this.budgets = Objects.requireNonNull(budgets, "budgets");
        this.auth = Objects.requireNonNull(auth, "auth");
        this.json = Objects.requireNonNull(json, "json");
        this.readRequestExecutor = Objects.requireNonNull(readRequestExecutor, "readRequestExecutor");
        this.endpointStats = Objects.requireNonNull(endpointStats, "endpointStats");
    }

    HttpHandler directBlocking(String endpointMetricKey,
                               String operation,
                               RouteBudget routeBudget,
                               HttpEndpointBudget endpointBudget,
                               boolean requireIngressAuth,
                               BlockingExchangeHandler handler) {
        return exchange -> {
            EndpointStats endpoint = endpointFor(endpointMetricKey, routeBudget, endpointBudget);
            if (!budgets.tryAcquire(exchange, operation, routeBudget, endpointBudget, endpoint, false)) {
                return;
            }
            if (auth.rejectUnauthorized(exchange, requireIngressAuth)) {
                budgets.release(routeBudget, endpointBudget);
                return;
            }
            exchange.dispatch(() -> {
                json.responseGuard(exchange);
                long startedAt = System.nanoTime();
                long endpointInflight = endpoint.inflight().incrementAndGet();
                endpoint.maxInflight().accumulateAndGet(endpointInflight, Math::max);
                try {
                    routeBudget.requestCount().incrementAndGet();
                    endpoint.requestCount().incrementAndGet();
                    handler.handle(exchange);
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
            });
        };
    }

    HttpHandler blocking(String endpointMetricKey,
                         String operation,
                         RouteBudget routeBudget,
                         HttpEndpointBudget endpointBudget,
                         boolean requireIngressAuth,
                         BlockingExchangeHandler handler) {
        return exchange -> {
            EndpointStats endpoint = endpointFor(endpointMetricKey, routeBudget, endpointBudget);
            if (!budgets.tryAcquire(exchange, operation, routeBudget, endpointBudget, endpoint, true)) {
                return;
            }
            if (auth.rejectUnauthorized(exchange, requireIngressAuth)) {
                budgets.release(routeBudget, endpointBudget);
                return;
            }
            exchange.dispatch(() -> {
                Future<?> future = null;
                json.responseGuard(exchange);
                long startedAt = System.nanoTime();
                long endpointInflight = endpoint.inflight().incrementAndGet();
                endpoint.maxInflight().accumulateAndGet(endpointInflight, Math::max);
                try {
                    routeBudget.requestCount().incrementAndGet();
                    endpoint.requestCount().incrementAndGet();
                    future = readRequestExecutor.submit(() -> {
                        try {
                            handler.handle(exchange);
                        } catch (IOException e) {
                            throw new WrappedIOException(e);
                        }
                    });
                    future.get(routeBudget.timeoutMillis(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    if (future != null) {
                        future.cancel(true);
                    }
                    routeBudget.timeoutCount().incrementAndGet();
                    endpoint.timeoutCount().incrementAndGet();
                    if (!exchange.isResponseStarted()) {
                        json.writeBestEffort(exchange, new RequestTimeoutException(
                                operation + " exceeded timeout " + routeBudget.timeoutMillis() + "ms"
                        ));
                    }
                } catch (RejectedExecutionException e) {
                    if (!exchange.isResponseStarted()) {
                        json.writeBestEffort(exchange, new OverloadedException("request executor is not accepting work"));
                    }
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    endpoint.failureCount().incrementAndGet();
                    if (cause instanceof WrappedIOException wrapped) {
                        json.writeBestEffort(exchange, wrapped.cause);
                    } else if (cause instanceof RuntimeException runtime) {
                        json.writeBestEffort(exchange, runtime);
                    } else if (!exchange.isResponseStarted()) {
                        json.writeBestEffort(exchange, new InternalServerException("http handler failed", cause));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    endpoint.failureCount().incrementAndGet();
                    if (!exchange.isResponseStarted()) {
                        json.writeBestEffort(exchange, new ServiceUnavailableException("http handler interrupted", e));
                    }
                } finally {
                    recordEndpointLatency(endpoint, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                    endpoint.inflight().decrementAndGet();
                    budgets.release(routeBudget, endpointBudget);
                }
            });
        };
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

    private static final class WrappedIOException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final IOException cause;

        private WrappedIOException(IOException cause) {
            super(cause);
            this.cause = cause;
        }
    }
}
