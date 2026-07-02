package io.github.ike.ullmatcher.sdk;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MatcherBinaryClientTest {
    @Test
    void submitOrdersEncodesBinaryFrameAndReadsResults() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    handleBinaryRequest(socket, 101L, 7L);
                }
                return null;
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1",
                    serverSocket.getLocalPort(),
                    Duration.ofSeconds(2))) {
                List<BinaryCommandResult> results = client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 101L, 99L, 2L)));

                assertEquals(1, results.size());
                assertEquals(101L, results.getFirst().orderId());
                assertEquals(7L, results.getFirst().sequence());
                assertEquals(0, results.getFirst().resultCode());
            }
            server.get();
        }
    }

    @Test
    void sequentialRequestsReuseOneSocket() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            AtomicInteger acceptedConnections = new AtomicInteger();
            FutureTask<Void> server = new FutureTask<>(() -> {
                try (Socket socket = serverSocket.accept()) {
                    acceptedConnections.incrementAndGet();
                    handleBinaryRequest(socket, 101L, 7L);
                    handleBinaryRequest(socket, 102L, 8L);
                }
                return null;
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1",
                    serverSocket.getLocalPort(),
                    Duration.ofSeconds(2))) {
                assertEquals(101L, client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 101L, 99L, 2L))).getFirst().orderId());
                assertEquals(102L, client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 102L, 100L, 3L))).getFirst().orderId());
                assertEquals(1, acceptedConnections.get());
            }
            server.get();
        }
    }

    @Test
    void reconnectClosesPreviousSocketBeforeOpeningNextOne() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Integer> server = new FutureTask<>(() -> {
                try (Socket first = serverSocket.accept()) {
                    int eofAfterClientReconnect = first.getInputStream().read();
                    try (Socket second = serverSocket.accept()) {
                        handleBinaryRequest(second, 201L, 9L);
                    }
                    return eofAfterClientReconnect;
                }
            });
            Thread.ofPlatform().start(server);

            try (MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1",
                    serverSocket.getLocalPort(),
                    Duration.ofSeconds(2))) {
                assertTrue(client.connected());
                client.reconnect();
                List<BinaryCommandResult> results = client.submitOrders(List.of(BinaryNewOrder.buyLimit(1L, 201L, 101L, 4L)));

                assertEquals(1, results.size());
                assertEquals(201L, results.getFirst().orderId());
            }
            assertEquals(-1, server.get());
        }
    }

    @Test
    void closeMarksClientClosed() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                Socket accepted = serverSocket.accept();
                accepted.close();
                return null;
            });
            Thread.ofPlatform().start(server);

            MatcherBinaryClient client = new MatcherBinaryClient(
                    "127.0.0.1",
                    serverSocket.getLocalPort(),
                    Duration.ofSeconds(2));
            client.close();
            assertFalse(client.connected());
            server.get();
        }
    }

    @Test
    void constructorRejectsInvalidConnectionBudget() {
        assertThrows(IllegalArgumentException.class,
                () -> new MatcherBinaryClient("127.0.0.1", 0, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new MatcherBinaryClient("127.0.0.1", 65_536, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new MatcherBinaryClient("127.0.0.1", 12345, Duration.ZERO));
    }

    private static void handleBinaryRequest(Socket socket, long expectedOrderId, long sequence) throws IOException {
        byte[] header = socket.getInputStream().readNBytes(16);
        ByteBuffer headerBuffer = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        assertEquals(0x554C4C42, headerBuffer.getInt());
        assertEquals(1, headerBuffer.getShort());
        assertEquals(1, headerBuffer.getShort());
        assertEquals(1, headerBuffer.getInt());
        assertEquals(48, headerBuffer.getInt());

        byte[] request = socket.getInputStream().readNBytes(48);
        ByteBuffer requestBuffer = ByteBuffer.wrap(request).order(ByteOrder.BIG_ENDIAN);
        assertEquals(1L, requestBuffer.getLong());
        assertEquals(expectedOrderId, requestBuffer.getLong());
        requestBuffer.getLong();
        requestBuffer.getLong();

        ByteBuffer response = ByteBuffer.allocate(40).order(ByteOrder.BIG_ENDIAN);
        response.putInt(0x554C4C52);
        response.putShort((short) 1);
        response.putShort((short) 101);
        response.putInt(1);
        response.putInt(24);
        response.putLong(expectedOrderId);
        response.putLong(sequence);
        response.putInt(0);
        response.putInt(0);
        socket.getOutputStream().write(response.array());
        socket.getOutputStream().flush();
    }
}
