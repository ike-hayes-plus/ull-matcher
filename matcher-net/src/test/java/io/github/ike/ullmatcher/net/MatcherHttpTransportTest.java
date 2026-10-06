package io.github.ike.ullmatcher.net;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

final class MatcherHttpTransportTest {
    @Test
    void sharedClientUsesProcessWideVirtualThreadExecutor() {
        HttpClient first = MatcherHttpTransport.newClient(Duration.ofSeconds(1));
        HttpClient second = MatcherHttpTransport.newClient(Duration.ofSeconds(2));
        assertSame(first.executor().orElseThrow(), second.executor().orElseThrow());
    }

    @Test
    void clientVersionDefaultsToHttp2() {
        String previous = System.getProperty("matcher.httpClientVersion");
        try {
            System.clearProperty("matcher.httpClientVersion");
            assertEquals(HttpClient.Version.HTTP_2, MatcherHttpTransport.clientVersion());
            System.setProperty("matcher.httpClientVersion", "HTTP_1_1");
            assertEquals(HttpClient.Version.HTTP_1_1, MatcherHttpTransport.clientVersion());
        } finally {
            if (previous == null) {
                System.clearProperty("matcher.httpClientVersion");
            } else {
                System.setProperty("matcher.httpClientVersion", previous);
            }
        }
    }
}
