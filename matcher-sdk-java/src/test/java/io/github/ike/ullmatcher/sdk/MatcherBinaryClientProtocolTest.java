package io.github.ike.ullmatcher.sdk;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.FutureTask;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖 binary ingress 客户端的握手、撤单编码、响应帧校验与连接重建行为。
 */
final class MatcherBinaryClientProtocolTest {
    private static final int REQUEST_MAGIC = 0x554C4C42;
    private static final int RESPONSE_MAGIC = 0x554C4C52;
    private static final int FRAME_HEADER_BYTES = 16;
    private static final int HANDSHAKE_BYTES = 32;

    @Test
    void apiKeyIsSentAsZeroPaddedHandshakeBeforeTheFirstFrame() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<byte[]> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    byte[] handshake = socket.getInputStream().readNBytes(HANDSHAKE_BYTES);
                    readRequestHeader(socket);
                    socket.getInputStream().readNBytes(48);
                    writeResultFrame(socket, new long[]{101L}, new long[]{7L});
                    return handshake;
                }
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2), "  ingress-secret  ")) {
                assertEquals(101L, client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 101L, 99L, 2L)))
                        .getFirst().orderId());
            }

            byte[] expected = new byte[HANDSHAKE_BYTES];
            byte[] raw = "ingress-secret".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(raw, 0, expected, 0, raw.length);
            assertArrayEquals(expected, server.get());
        }
    }

    @Test
    void blankApiKeyIsTreatedAsNoHandshake() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Integer> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    int magic = readRequestHeader(socket)[0];
                    socket.getInputStream().readNBytes(48);
                    writeResultFrame(socket, new long[]{101L}, new long[]{7L});
                    return magic;
                }
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2), "   ")) {
                client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 101L, 99L, 2L)));
            }

            assertEquals(REQUEST_MAGIC, server.get());
        }
    }

    @Test
    void apiKeyLongerThanTheHandshakeSlotIsRejectedAndTheSocketIsReleased() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Integer> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    return socket.getInputStream().read();
                }
            });
            Thread.ofPlatform().start(server);

            IOException error = assertThrows(IOException.class, () -> new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2), "k".repeat(33)));

            assertTrue(error.getMessage().contains("exceeds 32 bytes"), error.getMessage());
            assertEquals(-1, server.get(), "rejected handshake must not leave the socket half-open");
        }
    }

    @Test
    void cancelOrdersEncodesOneSixteenByteRecordPerOrder() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<int[]> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    int[] header = readRequestHeader(socket);
                    byte[] payload = socket.getInputStream().readNBytes(header[4]);
                    ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
                    assertEquals(101L, buffer.getLong());
                    assertEquals(0L, buffer.getLong());
                    assertEquals(102L, buffer.getLong());
                    assertEquals(0L, buffer.getLong());
                    writeResultFrame(socket, new long[]{101L, 102L}, new long[]{11L, 12L});
                    return header;
                }
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2))) {
                List<BinaryCommandResult> results = client.cancelOrders(List.of(101L, 102L));

                assertEquals(2, results.size());
                assertEquals(11L, results.get(0).sequence());
                assertEquals(102L, results.get(1).orderId());
            }

            int[] header = server.get();
            assertEquals(REQUEST_MAGIC, header[0]);
            assertEquals(2, header[2], "cancel batches use frame type 2");
            assertEquals(2, header[3]);
            assertEquals(32, header[4]);
        }
    }

    @Test
    void submitOrdersCommittedUsesFrameTypeThreeAndReadsReplicationFlag() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<int[]> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    int[] header = readRequestHeader(socket);
                    socket.getInputStream().readNBytes(header[4]);
                    writeHeader(socket, RESPONSE_MAGIC, (short) 1, (short) 101, 1, 24);
                    ByteBuffer payload = ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN);
                    payload.putLong(301L);
                    payload.putLong(15L);
                    payload.putInt(0);
                    payload.putInt(1);
                    socket.getOutputStream().write(payload.array());
                    socket.getOutputStream().flush();
                    return header;
                }
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2))) {
                BinaryCommandResult result = client.submitOrdersCommitted(
                        List.of(BinaryNewOrder.buyLimit(1L, 301L, 99L, 2L))).getFirst();
                assertEquals(301L, result.orderId());
                assertEquals(15L, result.sequence());
                assertTrue(result.replicationCommitted());
            }

            int[] header = server.get();
            assertEquals(REQUEST_MAGIC, header[0]);
            assertEquals(3, header[2], "committed new-order batches use frame type 3");
        }
    }

    @Test
    void emptyResultFrameYieldsNoResults() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    readRequestHeader(socket);
                    socket.getInputStream().readNBytes(16);
                    writeResultFrame(socket, new long[0], new long[0]);
                }
                return null;
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2))) {
                assertTrue(client.cancelOrders(List.of(101L)).isEmpty());
            }
            server.get();
        }
    }

    @Test
    void batchArgumentsAreValidatedBeforeAnythingIsWritten() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                serverSocket.accept().close();
                return null;
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2))) {
                assertThrows(IllegalArgumentException.class, () -> client.submitOrders(null));
                assertThrows(IllegalArgumentException.class, () -> client.submitOrders(List.of()));
                assertThrows(IllegalArgumentException.class, () -> client.cancelOrders(null));
                assertThrows(IllegalArgumentException.class, () -> client.cancelOrders(List.of()));
                assertThrows(IllegalArgumentException.class, () -> client.cancelOrders(List.of(0L)));
                assertThrows(IllegalArgumentException.class, () -> client.cancelOrders(List.of(101L, -1L)));
            }
            server.get();
        }
    }

    @Test
    void truncatedResponseHeaderIsRejected() throws Exception {
        IOException error = assertThrows(IOException.class, () -> exchangeWithBrokenServer(socket -> {
            socket.getOutputStream().write(new byte[8]);
            socket.getOutputStream().flush();
        }));

        assertEquals("binary ingress response header truncated", error.getMessage());
    }

    @Test
    void responseHeaderWithForeignMagicIsRejected() throws Exception {
        IOException error = assertThrows(IOException.class, () -> exchangeWithBrokenServer(socket ->
                writeHeader(socket, 0xDEADBEEF, (short) 1, (short) 101, 1, 24)));

        assertEquals("invalid binary ingress response header", error.getMessage());
    }

    @Test
    void responseHeaderWithUnexpectedVersionOrFrameTypeIsRejected() throws Exception {
        IOException wrongVersion = assertThrows(IOException.class, () -> exchangeWithBrokenServer(socket ->
                writeHeader(socket, RESPONSE_MAGIC, (short) 2, (short) 101, 1, 24)));
        IOException wrongFrameType = assertThrows(IOException.class, () -> exchangeWithBrokenServer(socket ->
                writeHeader(socket, RESPONSE_MAGIC, (short) 1, (short) 7, 1, 24)));

        assertEquals("invalid binary ingress response header", wrongVersion.getMessage());
        assertEquals("invalid binary ingress response header", wrongFrameType.getMessage());
    }

    @Test
    void responsePayloadSizeThatDoesNotMatchTheRecordCountIsRejected() throws Exception {
        IOException error = assertThrows(IOException.class, () -> exchangeWithBrokenServer(socket ->
                writeHeader(socket, RESPONSE_MAGIC, (short) 1, (short) 101, 1, 8)));

        assertEquals("invalid binary ingress response payload size", error.getMessage());
    }

    @Test
    void truncatedResponsePayloadIsRejected() throws Exception {
        IOException error = assertThrows(IOException.class, () -> exchangeWithBrokenServer(socket -> {
            writeHeader(socket, RESPONSE_MAGIC, (short) 1, (short) 101, 1, 24);
            socket.getOutputStream().write(new byte[10]);
            socket.getOutputStream().flush();
        }));

        assertEquals("binary ingress response payload truncated", error.getMessage());
    }

    @Test
    void protocolFailureDiscardsTheSocketAndTheNextCallReconnects() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                try (Socket broken = serverSocket.accept()) {
                    readRequestHeader(broken);
                    broken.getInputStream().readNBytes(48);
                    writeHeader(broken, 0xDEADBEEF, (short) 1, (short) 101, 1, 24);
                }
                try (Socket healthy = serverSocket.accept()) {
                    readRequestHeader(healthy);
                    healthy.getInputStream().readNBytes(48);
                    writeResultFrame(healthy, new long[]{202L}, new long[]{9L});
                }
                return null;
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2))) {
                assertThrows(IOException.class,
                        () -> client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 201L, 99L, 2L))));
                assertFalse(client.connected(), "failed socket must be discarded");

                List<BinaryCommandResult> results = client.submitOrders(
                        List.of(BinaryNewOrder.buyLimit(1L, 202L, 99L, 2L)));

                assertEquals(202L, results.getFirst().orderId());
                assertTrue(client.connected());
            }
            server.get();
        }
    }

    @Test
    void closedClientRejectsFurtherOperations() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                serverSocket.accept().close();
                return null;
            });
            Thread.ofPlatform().start(server);

            MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2));
            client.close();
            client.close();

            assertFalse(client.connected());
            assertEquals("binary client is closed",
                    assertThrows(IOException.class, client::reconnect).getMessage());
            assertEquals("binary client is closed", assertThrows(IOException.class,
                    () -> client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 101L, 99L, 2L)))).getMessage());
            server.get();
        }
    }

    @Test
    void constructorValidatesHostTimeoutAndPort() {
        assertThrows(NullPointerException.class, () -> new MatcherBinaryClient(null, 12345, Duration.ofSeconds(1)));
        assertThrows(NullPointerException.class, () -> new MatcherBinaryClient("127.0.0.1", 12345, null));
        assertThrows(IllegalArgumentException.class,
                () -> new MatcherBinaryClient("127.0.0.1", 12345, Duration.ofSeconds(-1)));
    }

    @Test
    void timeoutExposesTheConfiguredBudget() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                serverSocket.accept().close();
                return null;
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofMillis(900))) {
                assertEquals(Duration.ofMillis(900), client.timeout());
            }
            server.get();
        }
    }

    /**
     * 用一个只会回写坏响应的服务端跑一次提交，便于逐条验证响应帧的强校验分支。
     */
    private static void exchangeWithBrokenServer(BrokenResponder responder) throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    readRequestHeader(socket);
                    socket.getInputStream().readNBytes(48);
                    responder.respond(socket);
                }
                return null;
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1", serverSocket.getLocalPort(), Duration.ofSeconds(2))) {
                client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 101L, 99L, 2L)));
            } finally {
                server.get();
            }
        }
    }

    private static int[] readRequestHeader(Socket socket) throws IOException {
        byte[] header = socket.getInputStream().readNBytes(FRAME_HEADER_BYTES);
        assertEquals(FRAME_HEADER_BYTES, header.length);
        ByteBuffer buffer = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        return new int[]{buffer.getInt(), buffer.getShort(), buffer.getShort(), buffer.getInt(), buffer.getInt()};
    }

    private static void writeHeader(Socket socket, int magic, short version, short frameType, int count, int payloadBytes)
            throws IOException {
        ByteBuffer header = ByteBuffer.allocate(FRAME_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(magic);
        header.putShort(version);
        header.putShort(frameType);
        header.putInt(count);
        header.putInt(payloadBytes);
        socket.getOutputStream().write(header.array());
        socket.getOutputStream().flush();
    }

    private static void writeResultFrame(Socket socket, long[] orderIds, long[] sequences) throws IOException {
        writeHeader(socket, RESPONSE_MAGIC, (short) 1, (short) 101, orderIds.length, orderIds.length * 24);
        ByteBuffer payload = ByteBuffer.allocate(orderIds.length * 24).order(ByteOrder.BIG_ENDIAN);
        for (int i = 0; i < orderIds.length; i++) {
            payload.putLong(orderIds[i]);
            payload.putLong(sequences[i]);
            payload.putInt(0);
            payload.putInt(0);
        }
        socket.getOutputStream().write(payload.array());
        socket.getOutputStream().flush();
    }

    @FunctionalInterface
    private interface BrokenResponder {
        void respond(Socket socket) throws IOException;
    }
}
