package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class WriteAdmissionControllerTest {
    @Test
    void tenantBudgetRejectsSecondConcurrentSubmitForSameTenant() {
        WriteAdmissionController controller = new WriteAdmissionController("merchant:42", 4, 1, "X-Ull-Tenant-Key");
        HttpServerExchange first = new HttpServerExchange(null);
        first.getRequestHeaders().put(new io.undertow.util.HttpString("X-Ull-Tenant-Key"), "tenant-a");
        HttpServerExchange second = new HttpServerExchange(null);
        second.getRequestHeaders().put(new io.undertow.util.HttpString("X-Ull-Tenant-Key"), "tenant-a");

        WriteAdmissionController.Admission admission = controller.acquireForSubmit(first, 7L);
        try (admission) {
            OverloadedException error = assertThrows(OverloadedException.class, () -> controller.acquireForSubmit(second, 7L));
            assertEquals("tenant write budget exhausted; tenant=tenant-a limit=1 weight=1", error.getMessage());
            assertEquals(1L, controller.tenantOverloadCount());
        }
    }

    @Test
    void shardBudgetRejectsCancelWhenShardBudgetExhausted() {
        WriteAdmissionController controller = new WriteAdmissionController("merchant:42", 1, 0, "X-Ull-Tenant-Key");
        HttpServerExchange first = new HttpServerExchange(null);
        HttpServerExchange second = new HttpServerExchange(null);

        WriteAdmissionController.Admission admission = controller.acquireForCancel(first);
        try (admission) {
            OverloadedException error = assertThrows(OverloadedException.class, () -> controller.acquireForCancel(second));
            assertEquals("shard write budget exhausted; shard=merchant:42 limit=1", error.getMessage());
            assertEquals(1L, controller.shardOverloadCount());
        }
    }

    @Test
    void tenantWeightAllowsAdditionalConcurrentCapacity() {
        WriteAdmissionController controller = new WriteAdmissionController("merchant:42", new WriteAdmissionPolicyConfig(
                8, 1, "X-Ull-Tenant-Key", 0.0d, 0, 0.0d, 0, 1, "tenant-a=2", "X-Ull-Tenant-Priority"
        ));
        HttpServerExchange first = exchange("tenant-a", null);
        HttpServerExchange second = exchange("tenant-a", null);
        HttpServerExchange third = exchange("tenant-a", null);

        WriteAdmissionController.Admission firstAdmission = controller.acquireForSubmit(first, 7L);
        WriteAdmissionController.Admission secondAdmission = controller.acquireForSubmit(second, 7L);
        try (firstAdmission; secondAdmission) {
            OverloadedException error = assertThrows(OverloadedException.class, () -> controller.acquireForSubmit(third, 7L));
            assertEquals("tenant write budget exhausted; tenant=tenant-a limit=2 weight=2", error.getMessage());
        }
    }

    @Test
    void tenantRateLimitRejectsLowPriorityBurstEarlierThanHighPriority() {
        WriteAdmissionController controller = new WriteAdmissionController("merchant:42", new WriteAdmissionPolicyConfig(
                8, 4, "X-Ull-Tenant-Key", 0.0d, 0, 1.0d, 1, 1, "", "X-Ull-Tenant-Priority"
        ));

        WriteAdmissionController.Admission admission = controller.acquireForSubmit(exchange("tenant-a", "HIGH"), 7L);
        try (admission) {
            OverloadedException error = assertThrows(OverloadedException.class,
                    () -> controller.acquireForSubmit(exchange("tenant-a", "LOW"), 7L));
            assertEquals("tenant write rate limit exceeded; tenant=tenant-a rate=1.0/s burst=1 weight=1 priority=LOW", error.getMessage());
            assertEquals(1L, controller.tenantRateLimitedCount());
        }
    }

    private static HttpServerExchange exchange(String tenant, String priority) {
        HttpServerExchange exchange = new HttpServerExchange(null);
        if (tenant != null) {
            exchange.getRequestHeaders().put(new io.undertow.util.HttpString("X-Ull-Tenant-Key"), tenant);
        }
        if (priority != null) {
            exchange.getRequestHeaders().put(new io.undertow.util.HttpString("X-Ull-Tenant-Priority"), priority);
        }
        return exchange;
    }
}
