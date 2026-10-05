package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 编解码安全会话握手请求。
 */
public final class AeronSecureHandshakeRequestCodec {
    public static final int VERSION = 1;

    private static final int OFFSET_VERSION = 0;
    private static final int OFFSET_REQUEST_ID = OFFSET_VERSION + Integer.BYTES;
    private static final int OFFSET_SESSION_ID = OFFSET_REQUEST_ID + Long.BYTES;
    private static final int OFFSET_RESPONSE_STREAM_ID = OFFSET_SESSION_ID + Long.BYTES;
    private static final int OFFSET_CREATED_AT_MILLIS = OFFSET_RESPONSE_STREAM_ID + Integer.BYTES;
    private static final int OFFSET_EXPIRES_AT_MILLIS = OFFSET_CREATED_AT_MILLIS + Long.BYTES;
    private static final int OFFSET_NODE_ID_LENGTH = OFFSET_EXPIRES_AT_MILLIS + Long.BYTES;

    private AeronSecureHandshakeRequestCodec() {
    }

    public static UnsafeBuffer allocateBuffer(Request request) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength(request)));
    }

    public static int encodedLength(Request request) {
        int length = OFFSET_NODE_ID_LENGTH + Integer.BYTES;
        length += request.nodeId().getBytes(StandardCharsets.UTF_8).length;
        length += Integer.BYTES + request.responseChannel().getBytes(StandardCharsets.UTF_8).length;
        length += Integer.BYTES + request.clientNonce().length;
        length += Integer.BYTES + request.clientEphemeralPublicKey().length;
        length += Integer.BYTES;
        for (byte[] certificate : request.certificateChainDer()) {
            length += Integer.BYTES + certificate.length;
        }
        length += Integer.BYTES + request.signature().length;
        return length;
    }

    public static int encode(Request request, MutableDirectBuffer buffer) {
        byte[] nodeIdBytes = request.nodeId().getBytes(StandardCharsets.UTF_8);
        byte[] responseChannelBytes = request.responseChannel().getBytes(StandardCharsets.UTF_8);
        if (buffer.capacity() < encodedLength(request)) {
            throw new IllegalArgumentException("buffer capacity too small for secure handshake request");
        }
        int cursor = OFFSET_VERSION;
        buffer.putInt(cursor, VERSION);
        cursor += Integer.BYTES;
        buffer.putLong(cursor, request.requestId());
        cursor += Long.BYTES;
        buffer.putLong(cursor, request.sessionId());
        cursor += Long.BYTES;
        buffer.putInt(cursor, request.responseStreamId());
        cursor += Integer.BYTES;
        buffer.putLong(cursor, request.createdAtMillis());
        cursor += Long.BYTES;
        buffer.putLong(cursor, request.expiresAtMillis());
        cursor += Long.BYTES;
        cursor = putBytes(buffer, cursor, nodeIdBytes);
        cursor = putBytes(buffer, cursor, responseChannelBytes);
        cursor = putBytes(buffer, cursor, request.clientNonce());
        cursor = putBytes(buffer, cursor, request.clientEphemeralPublicKey());
        buffer.putInt(cursor, request.certificateChainDer().size());
        cursor += Integer.BYTES;
        for (byte[] certificate : request.certificateChainDer()) {
            cursor = putBytes(buffer, cursor, certificate);
        }
        cursor = putBytes(buffer, cursor, request.signature());
        return cursor;
    }

    public static Request decode(DirectBuffer buffer, int offset, int length) {
        int end = frameEnd(buffer, offset, length);
        if (length < OFFSET_NODE_ID_LENGTH) {
            throw new IllegalArgumentException("secure handshake request frame is truncated");
        }
        int cursor = offset;
        int version = buffer.getInt(cursor);
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported secure handshake request version " + version);
        }
        cursor += Integer.BYTES;
        long requestId = buffer.getLong(cursor);
        cursor += Long.BYTES;
        long sessionId = buffer.getLong(cursor);
        cursor += Long.BYTES;
        int responseStreamId = buffer.getInt(cursor);
        cursor += Integer.BYTES;
        long createdAtMillis = buffer.getLong(cursor);
        cursor += Long.BYTES;
        long expiresAtMillis = buffer.getLong(cursor);
        cursor += Long.BYTES;
        BytesValue nodeId = readBytes(buffer, cursor, end);
        cursor = nodeId.nextOffset();
        BytesValue responseChannel = readBytes(buffer, cursor, end);
        cursor = responseChannel.nextOffset();
        BytesValue clientNonce = readBytes(buffer, cursor, end);
        cursor = clientNonce.nextOffset();
        BytesValue clientPublicKey = readBytes(buffer, cursor, end);
        cursor = clientPublicKey.nextOffset();
        int certCount = readCount(buffer, cursor, end);
        cursor += Integer.BYTES;
        List<byte[]> certificateChain = new ArrayList<>(certCount);
        for (int i = 0; i < certCount; i++) {
            BytesValue certificate = readBytes(buffer, cursor, end);
            cursor = certificate.nextOffset();
            certificateChain.add(certificate.bytes());
        }
        BytesValue signature = readBytes(buffer, cursor, end);
        return new Request(
                requestId,
                sessionId,
                new String(nodeId.bytes(), StandardCharsets.UTF_8),
                new String(responseChannel.bytes(), StandardCharsets.UTF_8),
                responseStreamId,
                createdAtMillis,
                expiresAtMillis,
                clientNonce.bytes(),
                clientPublicKey.bytes(),
                List.copyOf(certificateChain),
                signature.bytes()
        );
    }

    private static int putBytes(MutableDirectBuffer buffer, int offset, byte[] bytes) {
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + Integer.BYTES, bytes);
        return offset + Integer.BYTES + bytes.length;
    }

    private static int frameEnd(DirectBuffer buffer, int offset, int length) {
        if (offset < 0 || length < 0 || offset > buffer.capacity() - length) {
            throw new IllegalArgumentException("secure handshake frame is outside the buffer");
        }
        return offset + length;
    }

    private static int readCount(DirectBuffer buffer, int offset, int end) {
        if (offset < 0 || offset > end - Integer.BYTES) {
            throw new IllegalArgumentException("secure handshake request frame is truncated");
        }
        int count = buffer.getInt(offset);
        int remaining = end - (offset + Integer.BYTES);
        if (count < 0 || count > remaining / Integer.BYTES) {
            throw new IllegalArgumentException("secure handshake certificate count exceeds frame");
        }
        return count;
    }

    private static BytesValue readBytes(DirectBuffer buffer, int offset, int end) {
        if (offset < 0 || offset > end - Integer.BYTES) {
            throw new IllegalArgumentException("secure handshake request frame is truncated");
        }
        int byteLength = buffer.getInt(offset);
        int payloadOffset = offset + Integer.BYTES;
        if (byteLength < 0 || byteLength > end - payloadOffset) {
            throw new IllegalArgumentException("secure handshake field length exceeds frame");
        }
        byte[] bytes = new byte[byteLength];
        buffer.getBytes(payloadOffset, bytes);
        return new BytesValue(bytes, payloadOffset + byteLength);
    }

    private record BytesValue(byte[] bytes, int nextOffset) {
    }

    public record Request(
            long requestId,
            long sessionId,
            String nodeId,
            String responseChannel,
            int responseStreamId,
            long createdAtMillis,
            long expiresAtMillis,
            byte[] clientNonce,
            byte[] clientEphemeralPublicKey,
            List<byte[]> certificateChainDer,
            byte[] signature
    ) {
    }
}
