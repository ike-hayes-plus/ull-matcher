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

import static org.junit.jupiter.api.Assertions.assertEquals;

final class MatcherBinaryClientTest {
    @Test
    void submitOrdersEncodesBinaryFrameAndReadsResults() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                handleBinaryConnection(serverSocket);
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

    private static void handleBinaryConnection(ServerSocket serverSocket) throws IOException {
        try (Socket socket = serverSocket.accept()) {
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
            assertEquals(101L, requestBuffer.getLong());
            assertEquals(99L, requestBuffer.getLong());
            assertEquals(2L, requestBuffer.getLong());

            ByteBuffer response = ByteBuffer.allocate(40).order(ByteOrder.BIG_ENDIAN);
            response.putInt(0x554C4C52);
            response.putShort((short) 1);
            response.putShort((short) 101);
            response.putInt(1);
            response.putInt(24);
            response.putLong(101L);
            response.putLong(7L);
            response.putInt(0);
            response.putInt(0);
            socket.getOutputStream().write(response.array());
            socket.getOutputStream().flush();
        }
    }
}
