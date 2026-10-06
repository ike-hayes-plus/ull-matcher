package io.github.ike.ullmatcher.server.api;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HTTP handler 执行绑定。默认走 {@link MatcherHttpExecutors} 共享虚拟线程池；
 * {@code -Dmatcher.httpPlatformReadExecutor=true} 时为该实例创建有界平台线程池。
 */
final class HttpDispatchExecutor implements AutoCloseable {
    private final Executor delegate;
    private final ThreadPoolExecutor ownedPlatformPool;
    private final boolean platformBoundedPool;
    private final int platformQueueCapacity;
    private HttpDispatchExecutor(Executor delegate,
                                   ThreadPoolExecutor ownedPlatformPool,
                                   boolean platformBoundedPool,
                                   int platformQueueCapacity) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.ownedPlatformPool = ownedPlatformPool;
        this.platformBoundedPool = platformBoundedPool;
        this.platformQueueCapacity = platformQueueCapacity;
    }

    static HttpDispatchExecutor create(int undertowWorkerThreads, int maxConcurrentRequests) {
        if (Boolean.getBoolean("matcher.httpPlatformReadExecutor")) {
            MatcherHttpExecutors.onHttpServerOpened(false);
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
        MatcherHttpExecutors.onHttpServerOpened(true);
        return new HttpDispatchExecutor(
                MatcherHttpExecutors.sharedVirtualDispatchExecutor(),
                null,
                false,
                0);
    }

    Executor executor() {
        return delegate;
    }

    boolean platformExecutorSaturationChecksEnabled() {
        return platformBoundedPool;
    }

    boolean isPlatformExecutorSaturated() {
        if (ownedPlatformPool == null) {
            return false;
        }
        return ownedPlatformPool.getActiveCount() >= ownedPlatformPool.getMaximumPoolSize()
                && ownedPlatformPool.getQueue().remainingCapacity() == 0;
    }

    int platformWorkerCount() {
        return ownedPlatformPool == null ? 0 : ownedPlatformPool.getMaximumPoolSize();
    }

    int executorQueueDepth() {
        return ownedPlatformPool == null ? 0 : ownedPlatformPool.getQueue().size();
    }

    int executorQueueCapacity() {
        return platformQueueCapacity;
    }

    @Override
    public void close() {
        MatcherHttpExecutors.onHttpServerClosed();
        if (ownedPlatformPool != null) {
            ownedPlatformPool.shutdownNow();
            try {
                ownedPlatformPool.awaitTermination(5L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
