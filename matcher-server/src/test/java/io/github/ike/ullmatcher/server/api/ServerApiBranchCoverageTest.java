/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ServerApiBranchCoverageTest {
    @Test
    void writeAdmissionPolicyConfigRejectsEachInvalidField() {
        WriteAdmissionPolicyConfig defaults = WriteAdmissionPolicyConfig.defaults();
        assertEquals("X-Ull-Tenant-Key", defaults.tenantAdmissionHeader());
        assertThrows(NullPointerException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, null, 0, 0, 0, 0, 1, "", "X-P"));
        assertThrows(NullPointerException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", 0, 0, 0, 0, 1, null, "X-P"));
        assertThrows(NullPointerException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", 0, 0, 0, 0, 1, "", null));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                -1, 0, "X-T", 0, 0, 0, 0, 1, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, -1, "X-T", 0, 0, 0, 0, 1, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", -1, 0, 0, 0, 1, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", 0, -1, 0, 0, 1, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", 0, 0, -1, 0, 1, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", 0, 0, 0, -1, 1, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", 0, 0, 0, 0, 0, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, " ", 0, 0, 0, 0, 1, "", "X-P"));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionPolicyConfig(
                1, 0, "X-T", 0, 0, 0, 0, 1, "", " "));
    }

    @Test
    void writeAdmissionControllerCoversNoopRateLimitAndOverrideParsing() {
        WriteAdmissionController unlimited = new WriteAdmissionController("shard-a", 0, 0, "X-Ull-Tenant-Key");
        WriteAdmissionController.Admission admission = unlimited.acquireForCancel(exchange(null, null));
        admission.close();
        assertEquals(0.0d, unlimited.shardSaturation());
        assertEquals(0L, unlimited.shardInflight());
        assertEquals("shard-a", unlimited.shardKey());
        assertEquals(0L, unlimited.activeTenantBudgets());
        assertEquals(1L, unlimited.anonymousTenantRequests());
        assertEquals(1, unlimited.tenantDefaultWeight());

        WriteAdmissionController shardRate = new WriteAdmissionController("shard-a", new WriteAdmissionPolicyConfig(
                4, 0, "X-Ull-Tenant-Key", 1.0d, 0, 0.0d, 0, 1, " ,a=2, ", "X-Ull-Tenant-Priority"));
        shardRate.acquireForSubmit(exchange("a", "CRITICAL"), 9L).close();
        assertThrows(OverloadedException.class, () -> shardRate.acquireForSubmit(exchange("a", "LOW"), 9L));
        assertEquals(1L, shardRate.shardRateLimitedCount());
        assertEquals(1.0d, shardRate.shardRateLimitPerSecond());
        assertEquals(0.0d, shardRate.tenantRateLimitPerSecond());

        WriteAdmissionController weights = new WriteAdmissionController("shard-a", new WriteAdmissionPolicyConfig(
                0, 2, "X-Ull-Tenant-Key", 0.0d, 0, 0.0d, 0, 3, "vip=4", "X-Ull-Tenant-Priority"));
        weights.acquireForSubmit(exchange("  ", "bogus"), 3L).close();
        weights.acquireForCancel(exchange(null, "")).close();

        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionController(
                "s", new WriteAdmissionPolicyConfig(1, 0, "X-T", 0, 0, 0, 0, 1, "bad", "X-P")));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionController(
                "s", new WriteAdmissionPolicyConfig(1, 0, "X-T", 0, 0, 0, 0, 1, "=2", "X-P")));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionController(
                "s", new WriteAdmissionPolicyConfig(1, 0, "X-T", 0, 0, 0, 0, 1, "a=", "X-P")));
        assertThrows(IllegalArgumentException.class, () -> new WriteAdmissionController(
                "s", new WriteAdmissionPolicyConfig(1, 0, "X-T", 0, 0, 0, 0, 1, "a=0", "X-P")));

        WriteAdmissionController saturating = new WriteAdmissionController("shard-a", 2, 0, "X-Ull-Tenant-Key");
        WriteAdmissionController.Admission held = saturating.acquireForSubmit(exchange("t", "HIGH"), 1L);
        try (held) {
            assertTrue(saturating.shardSaturation() > 0.0d);
        }
    }

    @Test
    void submitRequestPolicyPrefersQueryThenHeaderThenBody() {
        HttpServerExchange query = new HttpServerExchange(null);
        query.getQueryParameters().put("ack", new ArrayDeque<>(List.of("committed")));
        query.getRequestHeaders().put(new HttpString("X-Ull-Ack"), "local");
        assertEquals(HttpSubmitAckMode.COMMITTED, HttpSubmitRequestPolicy.resolveAckMode(
                query, "local", HttpSubmitAckMode.LOCAL));

        HttpServerExchange header = new HttpServerExchange(null);
        header.getQueryParameters().put("ack", new ArrayDeque<>(List.of("  ")));
        header.getRequestHeaders().put(new HttpString("X-Ull-Ack"), "committed");
        assertEquals(HttpSubmitAckMode.COMMITTED, HttpSubmitRequestPolicy.resolveAckMode(
                header, "local", HttpSubmitAckMode.LOCAL));

        HttpServerExchange body = new HttpServerExchange(null);
        assertEquals(HttpSubmitAckMode.LOCAL, HttpSubmitRequestPolicy.resolveAckMode(
                body, "LOCAL", HttpSubmitAckMode.COMMITTED));
        assertEquals(HttpSubmitAckMode.COMMITTED, HttpSubmitRequestPolicy.resolveAckMode(
                body, null, HttpSubmitAckMode.COMMITTED));
        assertThrows(BadRequestException.class, () -> HttpSubmitRequestPolicy.resolveAckMode(
                body, "nope", HttpSubmitAckMode.LOCAL));

        HttpServerExchange keyHeader = new HttpServerExchange(null);
        keyHeader.getRequestHeaders().put(new HttpString("Idempotency-Key"), " header-key ");
        assertEquals("header-key", HttpSubmitRequestPolicy.resolveIdempotencyKey(keyHeader, "body", "fallback"));
        assertEquals("body-key", HttpSubmitRequestPolicy.resolveIdempotencyKey(new HttpServerExchange(null), " body-key ", "fallback"));
        assertEquals("fallback", HttpSubmitRequestPolicy.resolveIdempotencyKey(new HttpServerExchange(null), "  ", "fallback"));
        assertEquals("new-order:1:2", HttpSubmitRequestPolicy.defaultOrderIdempotencyKey(1L, 2L));
        assertEquals("cancel-order:9", HttpSubmitRequestPolicy.defaultCancelIdempotencyKey(9L));
    }

    @Test
    void submitAckModeParsesCanonicalNamesOnly() {
        assertEquals(HttpSubmitAckMode.LOCAL, HttpSubmitAckMode.parse(" ", HttpSubmitAckMode.LOCAL));
        assertEquals(HttpSubmitAckMode.LOCAL, HttpSubmitAckMode.parse("local", HttpSubmitAckMode.COMMITTED));
        assertEquals(HttpSubmitAckMode.COMMITTED, HttpSubmitAckMode.parse("committed", HttpSubmitAckMode.LOCAL));
        assertThrows(BadRequestException.class, () -> HttpSubmitAckMode.parse("local-accepted", HttpSubmitAckMode.LOCAL));
        assertThrows(BadRequestException.class, () -> HttpSubmitAckMode.parse("replication-committed", HttpSubmitAckMode.LOCAL));
        assertTrue(HttpSubmitAckMode.values().length >= 2);
    }

    private static HttpServerExchange exchange(String tenant, String priority) {
        HttpServerExchange exchange = new HttpServerExchange(null);
        if (tenant != null) {
            exchange.getRequestHeaders().put(new HttpString("X-Ull-Tenant-Key"), tenant);
        }
        if (priority != null) {
            exchange.getRequestHeaders().put(Headers.AUTHORIZATION, "unused");
            exchange.getRequestHeaders().put(new HttpString("X-Ull-Tenant-Priority"), priority);
        }
        return exchange;
    }
}
