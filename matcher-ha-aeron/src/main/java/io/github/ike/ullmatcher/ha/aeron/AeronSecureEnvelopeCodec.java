package io.github.ike.ullmatcher.ha.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * 编解码携带会话编号与计数器的安全消息信封。
 */
public final class AeronSecureEnvelopeCodec {
    public static final int VERSION = 1;

    private static final int OFFSET_VERSION = 0;
    private static final int OFFSET_MESSAGE_TYPE = OFFSET_VERSION + Integer.BYTES;
    private static final int OFFSET_SESSION_ID = OFFSET_MESSAGE_TYPE + Integer.BYTES;
    private static final int OFFSET_COUNTER = OFFSET_SESSION_ID + Long.BYTES;
    private static final int OFFSET_CIPHERTEXT_LENGTH = OFFSET_COUNTER + Long.BYTES;
    private static final int OFFSET_CIPHERTEXT = OFFSET_CIPHERTEXT_LENGTH + Integer.BYTES;

    private AeronSecureEnvelopeCodec() {
    }

    public static UnsafeBuffer allocateBuffer(int ciphertextLength) {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(encodedLength(ciphertextLength)));
    }

    public static int encodedLength(int ciphertextLength) {
        return OFFSET_CIPHERTEXT + ciphertextLength;
    }

    public static int encode(int messageType,
                             long sessionId,
                             long counter,
                             byte[] ciphertext,
                             int ciphertextLength,
                             MutableDirectBuffer buffer) {
        if (ciphertextLength < 0 || ciphertextLength > ciphertext.length) {
            throw new IllegalArgumentException("invalid ciphertextLength " + ciphertextLength);
        }
        if (buffer.capacity() < encodedLength(ciphertextLength)) {
            throw new IllegalArgumentException("buffer capacity too small for secure envelope");
        }
        buffer.putInt(OFFSET_VERSION, VERSION);
        buffer.putInt(OFFSET_MESSAGE_TYPE, messageType);
        buffer.putLong(OFFSET_SESSION_ID, sessionId);
        buffer.putLong(OFFSET_COUNTER, counter);
        buffer.putInt(OFFSET_CIPHERTEXT_LENGTH, ciphertextLength);
        buffer.putBytes(OFFSET_CIPHERTEXT, ciphertext, 0, ciphertextLength);
        return encodedLength(ciphertextLength);
    }

    public static DecodedEnvelope decode(DirectBuffer buffer, int offset, int length) {
        int version = buffer.getInt(offset + OFFSET_VERSION);
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported secure envelope version " + version);
        }
        int messageType = buffer.getInt(offset + OFFSET_MESSAGE_TYPE);
        long sessionId = buffer.getLong(offset + OFFSET_SESSION_ID);
        long counter = buffer.getLong(offset + OFFSET_COUNTER);
        int ciphertextLength = buffer.getInt(offset + OFFSET_CIPHERTEXT_LENGTH);
        byte[] ciphertext = new byte[ciphertextLength];
        buffer.getBytes(offset + OFFSET_CIPHERTEXT, ciphertext);
        return new DecodedEnvelope(messageType, sessionId, counter, ciphertext);
    }

    public record DecodedEnvelope(int messageType, long sessionId, long counter, byte[] ciphertext) {
    }
}
