package io.github.ike.ullmatcher.server.api;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Dispatches blocking HTTP read handlers. Default: one virtual thread per request.
 * Set {@code -Dmatcher.httpPlatformReadExecutor=true} to restore the legacy bounded platform pool.
 */
final class HttpReadRequestExecutor implements AutoCloseable {
    private final ExecutorService delegate;
    private final ThreadPoolExecutor platformPool;
    private final boolean platformBoundedPool;
    private final int platformQueueCapacity;

    private HttpReadRequestExecutor(ExecutorService delegate,
                                      ThreadPoolExecutor platformPool,
                                      boolean platformBoundedPool,
                                      int platformQueueCapacity) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.platformPool = platformPool;
        this.platformBoundedPool = platformBoundedPool;
        this.platformQueueCapacity = platformQueueCapacity;
    }

    static HttpReadRequestExecutor create(int undertowWorkerThreads, int maxConcurrentRequests) {
        if (Boolean.getBoolean("matcher.httpPlatformReadExecutor")) {
            int requestThreads = Math.max(2, undertowWorkerThreads);
            int queueCapacity = Math.max(
                    requestThreads,
                    Math.min(maxConcurrentRequests, requestThreads * 2)
            );
            ThreadPoolExecutor pool = new ThreadPoolExecutor(
                    requestThreads,
                    requestThreads,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(queueCapacity),
                    Thread.ofPlatform().name("matcher-http-read-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy()
            );
            pool.prestartAllCoreThreads();
            return new HttpReadRequestExecutor(pool, pool, true, queueCapacity);
        }
        return new HttpReadRequestExecutor(
                Executors.newVirtualThreadPerTaskExecutor(),
                null,
                false,
                0
        );
    }

    Future<?> submit(Runnable task) {
        return delegate.submit(task);
    }

    boolean platformExecutorSaturationChecksEnabled() {
        return platformBoundedPool;
    }

    boolean isPlatformExecutorSaturated() {
        if (platformPool == null) {
            return false;
        }
        return platformPool.getActiveCount() >= platformPool.getMaximumPoolSize()
                && platformPool.getQueue().remainingCapacity() == 0;
    }

    int platformWorkerCount() {
        return platformPool == null ? 0 : platformPool.getMaximumPoolSize();
    }

    int executorQueueDepth() {
        return platformPool == null ? 0 : platformPool.getQueue().size();
    }

    int executorQueueCapacity() {
        return platformQueueCapacity;
    }

    @Override
    public void close() {
        delegate.shutdownNow();
        try {
            delegate.awaitTermination(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
