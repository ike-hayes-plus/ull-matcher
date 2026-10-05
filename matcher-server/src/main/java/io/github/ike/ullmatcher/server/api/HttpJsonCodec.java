/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JSON response writer and exception-to-payload mapping for the HTTP API.
 * Isolated from routing so handlers do not mix serialization with admission.
 */
final class HttpJsonCodec {
    static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    static final AttachmentKey<ResponseCommitGuard> RESPONSE_GUARD = AttachmentKey.create(ResponseCommitGuard.class);

    private static final Logger LOG = LoggerFactory.getLogger(HttpJsonCodec.class);

    private final JsonMapper objectMapper;
    private final MatcherServerMode serverMode;

    HttpJsonCodec(JsonMapper objectMapper, MatcherServerMode serverMode) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.serverMode = Objects.requireNonNull(serverMode, "serverMode");
    }

    void writeJson(HttpServerExchange exchange, int status, Object body) throws IOException {
        if (!responseGuard(exchange).tryCommit()) {
            return;
        }
        final byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(body);
        } catch (JacksonException e) {
            throw new IOException("failed to encode json response", e);
        }
        exchange.setStatusCode(status);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, JSON_CONTENT_TYPE);
        exchange.getResponseSender().send(java.nio.ByteBuffer.wrap(bytes));
    }

    void writeBestEffort(HttpServerExchange exchange, IOException error) {
        if (exchange.isResponseStarted()) {
            return;
        }
        try {
            handleServiceFailure(exchange, "http handler", error);
        } catch (IOException ignored) {
            exchange.setStatusCode(500);
            exchange.endExchange();
        }
    }

    void writeBestEffort(HttpServerExchange exchange, RuntimeException error) {
        if (exchange.isResponseStarted()) {
            return;
        }
        try {
            handleApiFailure(exchange, "http handler", error);
        } catch (IOException ignored) {
            exchange.setStatusCode(500);
            exchange.endExchange();
        }
    }

    void handleServiceFailure(HttpServerExchange exchange, String operation, IOException error) throws IOException {
        ServerApiException apiError = new ServiceUnavailableException(operation + " failed", error);
        logApiFailure(apiError, error);
        writeJson(exchange, apiError.statusCode(), serviceFailurePayload(serverMode, apiError, error));
    }

    void handleApiFailure(HttpServerExchange exchange, String operation, RuntimeException error) throws IOException {
        ServerApiException apiError = HttpApiExceptionMapper.map(operation, error);
        logApiFailure(apiError, error);
        writeJson(exchange, apiError.statusCode(), Map.of("error", apiError.getMessage(), "code", apiError.errorCode()));
    }

    static Map<String, Object> serviceFailurePayload(MatcherServerMode serverMode, ServerApiException apiError, Throwable error) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("error", apiError.getMessage());
        payload.put("code", apiError.errorCode());
        if (serverMode != MatcherServerMode.PROD) {
            payload.put("detail", error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage());
        }
        return payload;
    }

    ResponseCommitGuard responseGuard(HttpServerExchange exchange) {
        ResponseCommitGuard guard = exchange.getAttachment(RESPONSE_GUARD);
        if (guard == null) {
            guard = new ResponseCommitGuard();
            exchange.putAttachment(RESPONSE_GUARD, guard);
        }
        return guard;
    }

    static void logApiFailure(ServerApiException apiError, Throwable error) {
        if (apiError.logLevel() == Level.ERROR) {
            LOG.error(apiError.getMessage(), error);
        } else if (apiError.logLevel() == Level.WARN) {
            LOG.warn("{}: {}", apiError.getMessage(), error.getMessage());
        } else if (apiError.logLevel() == Level.INFO) {
            LOG.info(apiError.getMessage());
        } else {
            LOG.debug(apiError.getMessage());
        }
    }

    static final class ResponseCommitGuard {
        private final AtomicInteger committed = new AtomicInteger();

        boolean tryCommit() {
            return committed.compareAndSet(0, 1);
        }
    }
}
