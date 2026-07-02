package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.hft.WalDurabilityMode;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerMode;
import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.engine.TtlCancelConfig;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BinaryOrderIngressServerTest {
    @Test
    void selectorIngressAcceptsBatchAndReturnsSequences() throws Exception {
        Path dir = Files.createTempDirectory("binary-ingress-selector");
        MatcherServerConfig config = testConfig(dir);
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            try (BinaryOrderIngressServer server = new BinaryOrderIngressServer("127.0.0.1", 0, 32, nodeService)) {
                server.start();
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 5_000);
                    socket.setTcpNoDelay(true);
                    byte[] request = encodeNewOrderBatch();
                    socket.getOutputStream().write(request);
                    socket.getOutputStream().flush();

                    byte[] header = socket.getInputStream().readNBytes(16);
                    ByteBuffer headerBuffer = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
                    assertEquals(0x554C4C52, headerBuffer.getInt());
                    assertEquals(1, headerBuffer.getShort());
                    assertEquals(101, headerBuffer.getShort());
                    assertEquals(2, headerBuffer.getInt());
                    assertEquals(48, headerBuffer.getInt());

                    byte[] payload = socket.getInputStream().readNBytes(48);
                    ByteBuffer payloadBuffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
                    assertEquals(1001L, payloadBuffer.getLong());
                    long firstSequence = payloadBuffer.getLong();
                    assertEquals(0, payloadBuffer.getInt());
                    payloadBuffer.getInt();
                    assertEquals(1002L, payloadBuffer.getLong());
                    long secondSequence = payloadBuffer.getLong();
                    assertEquals(0, payloadBuffer.getInt());
                    assertEquals(0, payloadBuffer.getInt());
                    assertEquals(firstSequence + 1L, secondSequence);
                }
            }
        }
    }

    @Test
    void selectorIngressAcceptsPostOnlyTimeInForce() throws Exception {
        Path dir = Files.createTempDirectory("binary-ingress-post-only");
        MatcherServerConfig config = testConfig(dir);
        try (MatcherNodeService nodeService = new MatcherNodeService(config)) {
            nodeService.start();
            try (BinaryOrderIngressServer server = new BinaryOrderIngressServer("127.0.0.1", 0, 32, nodeService)) {
                server.start();
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 5_000);
                    socket.setTcpNoDelay(true);
                    byte[] request = encodeNewOrderBatch(1L, 2001L, 99L, 1L, (byte) 'P');
                    socket.getOutputStream().write(request);
                    socket.getOutputStream().flush();

                    byte[] header = socket.getInputStream().readNBytes(16);
                    ByteBuffer headerBuffer = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
                    assertEquals(0x554C4C52, headerBuffer.getInt());
                    assertEquals(1, headerBuffer.getShort());
                    assertEquals(101, headerBuffer.getShort());
                    assertEquals(1, headerBuffer.getInt());
                    assertEquals(24, headerBuffer.getInt());

                    byte[] payload = socket.getInputStream().readNBytes(24);
                    ByteBuffer payloadBuffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
                    assertEquals(2001L, payloadBuffer.getLong());
                    long sequence = payloadBuffer.getLong();
                    assertEquals(0, payloadBuffer.getInt());
                    assertEquals(0, payloadBuffer.getInt());
                    assertTrue(await(() -> nodeService.orderState(2001L) != null
                            && nodeService.orderState(2001L).sequence() == sequence, 5_000L));
                }
            }
        }
    }

    private static MatcherServerConfig testConfig(Path dir) {
        return new MatcherServerConfig(
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
                256,
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
                TtlCancelConfig.disabled(),
                HaRole.PRIMARY,
                io.github.ike.ullmatcher.runtime.MatchLoopConfig.defaults(),
                io.github.ike.ullmatcher.ha.standby.StandbySyncConfig.defaults(),
                null
        );
    }

    private static byte[] encodeNewOrderBatch() {
        ByteBuffer payload = ByteBuffer.allocate(96).order(ByteOrder.BIG_ENDIAN);
        encodeOrder(payload, 1L, 1001L, 100L, 1L, (byte) 'I');
        encodeOrder(payload, 1L, 1002L, 100L, 1L, (byte) 'I');
        return wrapNewOrderFrame(payload, 2);
    }

    private static byte[] encodeNewOrderBatch(long userId, long orderId, long price, long quantity, byte timeInForce) {
        ByteBuffer payload = ByteBuffer.allocate(48).order(ByteOrder.BIG_ENDIAN);
        encodeOrder(payload, userId, orderId, price, quantity, timeInForce);
        return wrapNewOrderFrame(payload, 1);
    }

    private static byte[] wrapNewOrderFrame(ByteBuffer payload, int count) {
        payload.flip();
        ByteBuffer frame = ByteBuffer.allocate(16 + payload.remaining()).order(ByteOrder.BIG_ENDIAN);
        frame.putInt(0x554C4C42);
        frame.putShort((short) 1);
        frame.putShort((short) 1);
        frame.putInt(count);
        frame.putInt(payload.remaining());
        frame.put(payload);
        return frame.array();
    }

    private static void encodeOrder(ByteBuffer payload, long userId, long orderId, long price, long quantity, byte timeInForce) {
        payload.putLong(userId);
        payload.putLong(orderId);
        payload.putLong(price);
        payload.putLong(quantity);
        payload.putLong(-1L);
        payload.put((byte) 'B');
        payload.put((byte) 'L');
        payload.put(timeInForce);
        payload.put(new byte[5]);
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
