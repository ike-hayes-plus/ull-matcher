package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StrictTtlRecoveryTest {
    @Test
    void explicitTtlPersistsAcrossSnapshotAndRestart() throws Exception {
        Path dir = Files.createTempDirectory("strict-ttl-recovery");
        MatcherServerConfig config = new MatcherServerConfig(
                MatcherServerMode.DEV,
                "node-a",
                "symbol-1",
                MatcherConfig.defaults(1),
                dir.resolve("wal"),
                "symbol-1",
                4L * 1024L * 1024L,
                WalDurabilityMode.SYNC_PER_COMMAND,
                1,
                0L,
                dir.resolve("snapshots").resolve("symbol-1.snap"),
                1 << 10,
                128,
                TimeUnit.MILLISECONDS.toNanos(200),
                0,
                "127.0.0.1",
                2,
                1 << 20,
                256,
                2_000L,
                128,
                96,
                16,
                2_000L,
                1_000L,
                5_000L,
                96,
                64,
                2,
                16,
                8,
                WriteAdmissionPolicyConfig.defaults(),
                false,
                0,
                GrpcReplicationServerConfig.defaults(0),
                ServerSecurityConfig.insecureDefaults(),
                new TtlCancelConfig(true, 25L, 0L, TimeUnit.SECONDS.toMillis(5), TimeUnit.SECONDS.toMillis(5), 16),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );

        long orderId = 42L;
        long ttlMillis = 200L;
        long persistedExpireAt;
        try (MatcherNodeService service = new MatcherNodeService(config)) {
            service.start();
            assertTrue(await(() -> service.health().acceptingClientCommands(), 5_000L));
            MatcherNodeService.SubmitResponse response = service.submitNewOrder(
                    7L, orderId, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L, ttlMillis
            );
            assertEquals(SubmitResult.ACCEPTED, response.result());
            assertTrue(await(() -> service.liveOrderCount() == 1, 5_000L));
            assertTrue(await(() -> service.orderState(orderId) != null, 5_000L));
            persistedExpireAt = service.orderState(orderId).expireAtEpochMillis();
            assertTrue(persistedExpireAt > 0L);
            service.createSnapshot();
        }

        long sleepMillis = Math.max(1L, persistedExpireAt - System.currentTimeMillis() + 50L);
        Thread.sleep(sleepMillis);

        try (MatcherNodeService restarted = new MatcherNodeService(config)) {
            restarted.start();
            assertTrue(await(() -> {
                OrderStateView view = restarted.orderState(orderId);
                return view != null && "CANCELLED".equals(view.status()) && restarted.liveOrderCount() == 0;
            }, 5_000L));
            OrderStateView view = restarted.orderState(orderId);
            assertNotNull(view);
            assertEquals("CANCELLED", view.status());
            assertEquals(persistedExpireAt, view.expireAtEpochMillis());
        }
    }

    private static boolean await(Check check, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (check.ok()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }
}
