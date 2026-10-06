package io.github.ike.ullmatcher.server.api;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 进程内 HTTP 并发资源统一入口：共享虚拟线程 dispatch、route 超时调度与引用计数生命周期。
 */
final class MatcherHttpExecutors {
    private static final Object LOCK = new Object();
    private static ExecutorService sharedVirtualDispatch;
    private static ScheduledExecutorService sharedRouteTimeouts;
    private static int httpServerInstances;

    private MatcherHttpExecutors() {
    }

    static void onHttpServerOpened(boolean useSharedVirtualDispatch) {
        synchronized (LOCK) {
            httpServerInstances++;
            if (useSharedVirtualDispatch) {
                ensureSharedVirtualDispatchLocked();
            }
        }
    }

    static void onHttpServerClosed() {
        synchronized (LOCK) {
            if (httpServerInstances <= 0) {
                return;
            }
            httpServerInstances--;
            if (httpServerInstances == 0) {
                shutdownQuietly(sharedVirtualDispatch);
                sharedVirtualDispatch = null;
                shutdownQuietly(sharedRouteTimeouts);
                sharedRouteTimeouts = null;
            }
        }
    }

    static Executor sharedVirtualDispatchExecutor() {
        synchronized (LOCK) {
            ensureSharedVirtualDispatchLocked();
            return sharedVirtualDispatch;
        }
    }

    static ScheduledExecutorService routeTimeoutScheduler() {
        synchronized (LOCK) {
            ensureRouteTimeoutSchedulerLocked();
            return sharedRouteTimeouts;
        }
    }

    static int httpServerInstancesForTests() {
        synchronized (LOCK) {
            return httpServerInstances;
        }
    }

    private static void ensureSharedVirtualDispatchLocked() {
        if (sharedVirtualDispatch == null) {
            sharedVirtualDispatch = Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("matcher-http-", 0).factory());
        }
    }

    private static void ensureRouteTimeoutSchedulerLocked() {
        if (sharedRouteTimeouts == null) {
            sharedRouteTimeouts = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().name("matcher-http-timeout-", 0).daemon().factory());
        }
    }

    private static void shutdownQuietly(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
