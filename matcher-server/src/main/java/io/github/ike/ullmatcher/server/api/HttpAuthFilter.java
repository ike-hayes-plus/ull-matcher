/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import io.undertow.server.HttpServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Shared-secret gate applied after a request has acquired its concurrency budgets.
 */
final class HttpAuthFilter {
    private static final Logger LOG = LoggerFactory.getLogger(HttpAuthFilter.class);

    private final IngressAuthConfig ingressAuthConfig;
    private final HttpJsonCodec json;

    HttpAuthFilter(IngressAuthConfig ingressAuthConfig, HttpJsonCodec json) {
        this.ingressAuthConfig = Objects.requireNonNull(ingressAuthConfig, "ingressAuthConfig");
        this.json = Objects.requireNonNull(json, "json");
    }

    /**
     * @return {@code true} when the exchange was rejected and the caller must release budgets
     */
    boolean rejectUnauthorized(HttpServerExchange exchange, boolean requireIngressAuth) {
        if (!requireIngressAuth || ingressAuthConfig.authorize(exchange)) {
            return false;
        }
        try {
            json.writeJson(exchange, 401, Map.of("error", "unauthorized", "code", "ingress_auth_required"));
        } catch (IOException e) {
            LOG.warn("failed to write unauthorized response", e);
        }
        return true;
    }
}
