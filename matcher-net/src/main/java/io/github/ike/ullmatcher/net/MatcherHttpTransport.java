package io.github.ike.ullmatcher.net;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * 进程内统一的 JDK {@link HttpClient} 工厂：底层 NIO 多路复用、默认 HTTP/2、虚拟线程执行异步回调。
 */
public final class MatcherHttpTransport {
    private static final Executor SHARED_VIRTUAL_EXECUTOR = java.util.concurrent.Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("matcher-http-client-", 0).factory());

    private MatcherHttpTransport() {
    }

    public static HttpClient newClient(Duration connectTimeout) {
        return newClientBuilder(connectTimeout).build();
    }

    public static HttpClient.Builder newClientBuilder(Duration connectTimeout) {
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        return HttpClient.newBuilder()
                .executor(SHARED_VIRTUAL_EXECUTOR)
                .connectTimeout(connectTimeout)
                .version(clientVersion());
    }

    static HttpClient.Version clientVersion() {
        String raw = System.getProperty("matcher.httpClientVersion", "HTTP_2");
        if ("HTTP_1_1".equalsIgnoreCase(raw)) {
            return HttpClient.Version.HTTP_1_1;
        }
        return HttpClient.Version.HTTP_2;
    }
}
