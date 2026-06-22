package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 编解码安全会话握手响应。
 */
public final class AeronSecureHandshakeResponseCodec {
    public static final int VERSION = 1;

    private static final int OFFSET_VERSION = 0;
    private static final int OFFSET_REQUEST_ID = OFFSET_VERSION + Integer.BYTES;
    private static final int OFFSET_SESSION_ID = OFFSET_REQUEST_ID + Long.BYTES;
    private static final int OFFSET_ACCEPTED = OFFSET_SESSION_ID + Long.BYTES;
    private static final int OFFSET_CREATED_AT_MILLIS = OFFSET_ACCEPTED + Integer.BYTES;
    private static final int OFFSET_EXPIRES_AT_MILLIS = OFFSET_CREATED_AT_MILLIS + Long.BYTES;
    private static final int OFFSET_ERROR_LENGTH = OFFSET_EXPIRES_AT_MILLIS + Long.BYTES;

    private AeronSecureHandshakeResponseCodec() {
    }

    public static UnsafeBuffer allocateBuffer(Response response) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength(response)));
    }

    public static int encodedLength(Response response) {
        int length = OFFSET_ERROR_LENGTH + Integer.BYTES + response.errorMessage().getBytes(StandardCharsets.UTF_8).length;
        if (!response.accepted()) {
            return length;
        }
        length += Integer.BYTES + response.nodeId().getBytes(StandardCharsets.UTF_8).length;
        length += Integer.BYTES + response.serverNonce().length;
        length += Integer.BYTES + response.serverEphemeralPublicKey().length;
        length += Integer.BYTES;
        for (byte[] certificate : response.certificateChainDer()) {
            length += Integer.BYTES + certificate.length;
        }
        length += Integer.BYTES + response.signature().length;
        return length;
    }

    public static int encode(Response response, MutableDirectBuffer buffer) {
        byte[] errorBytes = response.errorMessage().getBytes(StandardCharsets.UTF_8);
        if (buffer.capacity() < encodedLength(response)) {
            throw new IllegalArgumentException("buffer capacity too small for secure handshake response");
        }
        int cursor = OFFSET_VERSION;
        buffer.putInt(cursor, VERSION);
        cursor += Integer.BYTES;
        buffer.putLong(cursor, response.requestId());
        cursor += Long.BYTES;
        buffer.putLong(cursor, response.sessionId());
        cursor += Long.BYTES;
        buffer.putInt(cursor, response.accepted() ? 1 : 0);
        cursor += Integer.BYTES;
        buffer.putLong(cursor, response.createdAtMillis());
        cursor += Long.BYTES;
        buffer.putLong(cursor, response.expiresAtMillis());
        cursor += Long.BYTES;
        cursor = putBytes(buffer, cursor, errorBytes);
        if (!response.accepted()) {
            return cursor;
        }
        cursor = putBytes(buffer, cursor, response.nodeId().getBytes(StandardCharsets.UTF_8));
        cursor = putBytes(buffer, cursor, response.serverNonce());
        cursor = putBytes(buffer, cursor, response.serverEphemeralPublicKey());
        buffer.putInt(cursor, response.certificateChainDer().size());
        cursor += Integer.BYTES;
        for (byte[] certificate : response.certificateChainDer()) {
            cursor = putBytes(buffer, cursor, certificate);
        }
        cursor = putBytes(buffer, cursor, response.signature());
        return cursor;
    }

    public static Response decode(DirectBuffer buffer, int offset, int length) {
        int cursor = offset;
        int version = buffer.getInt(cursor);
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported secure handshake response version " + version);
        }
        cursor += Integer.BYTES;
        long requestId = buffer.getLong(cursor);
        cursor += Long.BYTES;
        long sessionId = buffer.getLong(cursor);
        cursor += Long.BYTES;
        boolean accepted = buffer.getInt(cursor) == 1;
        cursor += Integer.BYTES;
        long createdAtMillis = buffer.getLong(cursor);
        cursor += Long.BYTES;
        long expiresAtMillis = buffer.getLong(cursor);
        cursor += Long.BYTES;
        BytesValue error = readBytes(buffer, cursor);
        cursor = error.nextOffset();
        if (!accepted) {
            return new Response(
                    requestId,
                    sessionId,
                    false,
                    "",
                    createdAtMillis,
                    expiresAtMillis,
                    new String(error.bytes(), StandardCharsets.UTF_8),
                    new byte[0],
                    new byte[0],
                    List.of(),
                    new byte[0]
            );
        }
        BytesValue nodeId = readBytes(buffer, cursor);
        cursor = nodeId.nextOffset();
        BytesValue serverNonce = readBytes(buffer, cursor);
        cursor = serverNonce.nextOffset();
        BytesValue serverPublicKey = readBytes(buffer, cursor);
        cursor = serverPublicKey.nextOffset();
        int certCount = buffer.getInt(cursor);
        cursor += Integer.BYTES;
        List<byte[]> certificateChain = new ArrayList<>(certCount);
        for (int i = 0; i < certCount; i++) {
            BytesValue certificate = readBytes(buffer, cursor);
            cursor = certificate.nextOffset();
            certificateChain.add(certificate.bytes());
        }
        BytesValue signature = readBytes(buffer, cursor);
        return new Response(
                requestId,
                sessionId,
                true,
                new String(nodeId.bytes(), StandardCharsets.UTF_8),
                createdAtMillis,
                expiresAtMillis,
                "",
                serverNonce.bytes(),
                serverPublicKey.bytes(),
                List.copyOf(certificateChain),
                signature.bytes()
        );
    }

    private static int putBytes(MutableDirectBuffer buffer, int offset, byte[] bytes) {
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + Integer.BYTES, bytes);
        return offset + Integer.BYTES + bytes.length;
    }

    private static BytesValue readBytes(DirectBuffer buffer, int offset) {
        int length = buffer.getInt(offset);
        byte[] bytes = new byte[length];
        buffer.getBytes(offset + Integer.BYTES, bytes);
        return new BytesValue(bytes, offset + Integer.BYTES + length);
    }

    private record BytesValue(byte[] bytes, int nextOffset) {
    }

    public record Response(
            long requestId,
            long sessionId,
            boolean accepted,
            String nodeId,
            long createdAtMillis,
            long expiresAtMillis,
            String errorMessage,
            byte[] serverNonce,
            byte[] serverEphemeralPublicKey,
            List<byte[]> certificateChainDer,
            byte[] signature
    ) {
    }
}
