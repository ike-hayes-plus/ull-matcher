package io.github.ike.ullmatcher.server.api;

import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class HttpRouteTimeoutGuard {
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("matcher-http-timeout-", 0).daemon().factory());

    @FunctionalInterface
    interface BlockingWork {
        void run() throws IOException;
    }

    private HttpRouteTimeoutGuard() {
    }

    static void run(HttpServerExchange exchange,
                    long timeoutMillis,
                    String operation,
                    HttpJsonCodec json,
                    RouteBudget routeBudget,
                    EndpointStats endpoint,
                    BlockingWork work) throws IOException {
        if (timeoutMillis <= 0L) {
            work.run();
            return;
        }
        ScheduledFuture<?> timeoutTask = TIMEOUT_SCHEDULER.schedule(() -> {
            if (exchange.isResponseStarted()) {
                return;
            }
            routeBudget.timeoutCount().incrementAndGet();
            endpoint.timeoutCount().incrementAndGet();
            json.writeBestEffort(exchange, new RequestTimeoutException(
                    operation + " exceeded timeout " + timeoutMillis + "ms"));
        }, timeoutMillis, TimeUnit.MILLISECONDS);
        try {
            work.run();
        } finally {
            timeoutTask.cancel(false);
        }
    }
}
