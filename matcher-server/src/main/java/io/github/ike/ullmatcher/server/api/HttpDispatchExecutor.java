package io.github.ike.ullmatcher.server.api;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Executor for HTTP handler work. Default: virtual-thread-per-task.
 * {@code -Dmatcher.httpPlatformReadExecutor=true} selects a bounded platform pool (legacy).
 */
final class HttpDispatchExecutor implements AutoCloseable {
    private final ExecutorService delegate;
    private final ThreadPoolExecutor platformPool;
    private final boolean platformBoundedPool;
    private final int platformQueueCapacity;

    private HttpDispatchExecutor(ExecutorService delegate,
                                   ThreadPoolExecutor platformPool,
                                   boolean platformBoundedPool,
                                   int platformQueueCapacity) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.platformPool = platformPool;
        this.platformBoundedPool = platformBoundedPool;
        this.platformQueueCapacity = platformQueueCapacity;
    }

    static HttpDispatchExecutor create(int undertowWorkerThreads, int maxConcurrentRequests) {
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
                    Thread.ofPlatform().name("matcher-http-dispatch-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy()
            );
            pool.prestartAllCoreThreads();
            return new HttpDispatchExecutor(pool, pool, true, queueCapacity);
        }
        return new HttpDispatchExecutor(
                Executors.newVirtualThreadPerTaskExecutor(),
                null,
                false,
                0
        );
    }

    Executor executor() {
        return delegate;
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
