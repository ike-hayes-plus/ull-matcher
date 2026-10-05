/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HttpJsonCodecTest {
    @Test
    void prodPayloadOmitsInternalDetail() {
        ServiceUnavailableException apiError = new ServiceUnavailableException(
                "submit order failed", new java.io.IOException("wal path leaked"));

        Map<String, Object> payload = HttpJsonCodec.serviceFailurePayload(MatcherServerMode.PROD, apiError, apiError.getCause());

        assertEquals("submit order failed", payload.get("error"));
        assertFalse(payload.containsKey("detail"));
    }

    @Test
    void devPayloadKeepsTheCauseMessage() {
        ServiceUnavailableException apiError = new ServiceUnavailableException(
                "submit order failed", new java.io.IOException("wal path leaked"));

        Map<String, Object> payload = HttpJsonCodec.serviceFailurePayload(MatcherServerMode.DEV, apiError, apiError.getCause());

        assertEquals("wal path leaked", payload.get("detail"));
        assertTrue(payload.containsKey("code"));
    }

    @Test
    void serviceFailurePayloadFallsBackToExceptionTypeWhenCauseHasNoMessage() {
        ServiceUnavailableException apiError = new ServiceUnavailableException(
                "submit order failed", new java.io.IOException());

        Map<String, Object> payload = HttpJsonCodec.serviceFailurePayload(
                MatcherServerMode.DEV, apiError, apiError.getCause());

        assertEquals("IOException", payload.get("detail"));
    }

    @Test
    void logApiFailureCoversEveryDeclaredLevel() {
        HttpJsonCodec.logApiFailure(new InternalServerException("boom", new RuntimeException("cause")), new RuntimeException("cause"));
        HttpJsonCodec.logApiFailure(new OverloadedException("full"), new OverloadedException("full"));
        HttpJsonCodec.logApiFailure(new ConflictException("dup"), new ConflictException("dup"));
        HttpJsonCodec.logApiFailure(new BadRequestException("bad"), new BadRequestException("bad"));
        assertEquals("conflict", HttpApiExceptionMapper.map("submit", new IllegalStateException(" ")).errorCode());
        assertEquals("internal_error", HttpApiExceptionMapper.map("submit", new NullPointerException("npe")).errorCode());
    }
}
