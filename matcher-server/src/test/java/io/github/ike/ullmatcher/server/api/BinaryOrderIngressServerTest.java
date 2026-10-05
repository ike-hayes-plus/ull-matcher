package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import io.github.ike.ullmatcher.server.security.IngressAuthConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        return MatcherServerConfig.builder("node-a", 1, dir)
                .ringCapacity(1 << 10)
                .gatewaySpinLimit(128)
                .gatewayOfferTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(200))
                .walSegmentSizeBytes(4L * 1024L * 1024L)
                .httpPort(0)
                .httpWorkerThreads(2)
                .httpMaxBodyBytes(256)
                .httpMaxConcurrentRequests(256)
                .grpcPort(0)
                .grpcServerConfig(GrpcReplicationServerConfig.defaults(0))
                .build();
    }

    @Test
    void handshakeGrantsAccessAndWrongKeyClosesTheConnection() throws Exception {
        Path dir = Files.createTempDirectory("binary-ingress-auth");
        IngressAuthConfig auth = IngressAuthConfig.fromCommaSeparated("secret-key", null);
        try (MatcherNodeService nodeService = new MatcherNodeService(testConfig(dir))) {
            nodeService.start();
            try (BinaryOrderIngressServer server =
                         new BinaryOrderIngressServer("127.0.0.1", 0, 32, auth, nodeService)) {
                server.start();

                try (Socket socket = connect(server.port())) {
                    socket.getOutputStream().write(IngressAuthConfig.padHandshakeBytes("secret-key"));
                    socket.getOutputStream().write(encodeNewOrderBatch());
                    socket.getOutputStream().flush();

                    ByteBuffer header = ByteBuffer.wrap(socket.getInputStream().readNBytes(16)).order(ByteOrder.BIG_ENDIAN);
                    assertEquals(0x554C4C52, header.getInt());
                }

                try (Socket socket = connect(server.port())) {
                    socket.getOutputStream().write(IngressAuthConfig.padHandshakeBytes("wrong-key"));
                    socket.getOutputStream().write(encodeNewOrderBatch());
                    socket.getOutputStream().flush();

                    assertConnectionClosed(socket, "server must close the connection");
                }
                assertTrue(await(() -> server.connectionMetrics().handshakeFailures() == 1L, 5_000L));
            }
        }
    }

    @Test
    void unauthenticatedConnectionIsClosedWhenTheHandshakeTimesOut() throws Exception {
        Path dir = Files.createTempDirectory("binary-ingress-handshake-timeout");
        IngressAuthConfig auth = IngressAuthConfig.fromCommaSeparated("secret-key", null);
        BinaryIngressLimits limits = new BinaryIngressLimits(16, 200L, 0L);
        try (MatcherNodeService nodeService = new MatcherNodeService(testConfig(dir))) {
            nodeService.start();
            try (BinaryOrderIngressServer server =
                         new BinaryOrderIngressServer("127.0.0.1", 0, 32, auth, limits, nodeService)) {
                server.start();
                try (Socket socket = connect(server.port())) {
                    // Send a partial handshake so the session stays unauthenticated.
                    socket.getOutputStream().write(new byte[]{1, 2, 3});
                    socket.getOutputStream().flush();

                    assertConnectionClosed(socket, "server must reap the stalled handshake");
                }
                assertTrue(await(() -> server.connectionMetrics().handshakeTimeouts() >= 1L, 5_000L));
            }
        }
    }

    @Test
    void idleAuthenticatedConnectionIsReaped() throws Exception {
        Path dir = Files.createTempDirectory("binary-ingress-idle-timeout");
        BinaryIngressLimits limits = new BinaryIngressLimits(16, 5_000L, 200L);
        try (MatcherNodeService nodeService = new MatcherNodeService(testConfig(dir))) {
            nodeService.start();
            try (BinaryOrderIngressServer server = new BinaryOrderIngressServer(
                    "127.0.0.1", 0, 32, IngressAuthConfig.disabled(), limits, nodeService)) {
                server.start();
                try (Socket socket = connect(server.port())) {
                    assertConnectionClosed(socket, "idle session must be closed");
                }
                assertTrue(await(() -> server.connectionMetrics().idleTimeouts() >= 1L, 5_000L));
            }
        }
    }

    @Test
    void connectionsBeyondTheCeilingAreRejected() throws Exception {
        Path dir = Files.createTempDirectory("binary-ingress-max-connections");
        BinaryIngressLimits limits = new BinaryIngressLimits(1, 5_000L, 0L);
        try (MatcherNodeService nodeService = new MatcherNodeService(testConfig(dir))) {
            nodeService.start();
            try (BinaryOrderIngressServer server = new BinaryOrderIngressServer(
                    "127.0.0.1", 0, 32, IngressAuthConfig.disabled(), limits, nodeService)) {
                server.start();
                try (Socket first = connect(server.port())) {
                    assertTrue(await(() -> server.connectionMetrics().openConnections() == 1, 5_000L));
                    try (Socket second = connect(server.port())) {
                        assertConnectionClosed(second, "second connection must be rejected");
                    }
                    assertTrue(await(() -> server.connectionMetrics().rejectedConnections() >= 1L, 5_000L));
                    assertTrue(first.isConnected());
                }
            }
        }
    }

    @Test
    void limitsRejectNonPositiveValues() {
        assertThrows(IllegalArgumentException.class, () -> new BinaryIngressLimits(0, 1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new BinaryIngressLimits(1, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new BinaryIngressLimits(1, 1L, -1L));
        assertTrue(BinaryIngressLimits.defaults().idleTimeoutEnabled());
        assertFalse(new BinaryIngressLimits(1, 1L, 0L).idleTimeoutEnabled());
    }

    /**
     * Asserts the peer dropped the connection. A server close with unread client bytes still in
     * flight surfaces as RST rather than a clean EOF, so both outcomes count as closed.
     */
    private static void assertConnectionClosed(Socket socket, String message) throws IOException {
        try {
            assertEquals(-1, socket.getInputStream().read(), message);
        } catch (SocketException expected) {
            // connection reset by peer
        }
    }

    private static Socket connect(int port) throws Exception {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(10_000);
        return socket;
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
