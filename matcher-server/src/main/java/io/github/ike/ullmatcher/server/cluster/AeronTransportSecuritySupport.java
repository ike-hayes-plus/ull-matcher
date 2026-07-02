package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeRequestCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeResponseCodec;
import io.github.ike.ullmatcher.server.security.TransportSecurityLoader;
import io.github.ike.ullmatcher.server.security.TransportSecurityMaterials;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.security.spec.NamedParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 封装 Aeron 权威传输使用的握手、会话密钥派生与 AEAD 编解码。
 * 这里保持纯算法职责，避免把协议细节散落在收发两端实现中。
 */
final class AeronTransportSecuritySupport {
    private static final int AES_KEY_BYTES = 32;
    private static final int GCM_TAG_BITS = 128;
    private static final int NONCE_BYTES = 12;
    private static final int CLIENT_TO_SERVER_NONCE_PREFIX = 0x434C5453;
    private static final int SERVER_TO_CLIENT_NONCE_PREFIX = 0x53525643;
    private static final long CLOCK_SKEW_MILLIS = 30_000L;

    private AeronTransportSecuritySupport() {
    }

    static KeyPair newEphemeralKeyPair() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("X25519");
        generator.initialize(new NamedParameterSpec("X25519"));
        return generator.generateKeyPair();
    }

    static PublicKey decodeX25519PublicKey(byte[] encoded) throws GeneralSecurityException {
        return KeyFactory.getInstance("X25519").generatePublic(new X509EncodedKeySpec(encoded));
    }

    static byte[] signHandshakeRequest(AeronSecureHandshakeRequestCodec.Request request, PrivateKey privateKey, PublicKey certificateKey)
            throws GeneralSecurityException {
        Signature signature = Signature.getInstance(TransportSecurityLoader.signatureAlgorithm(certificateKey));
        signature.initSign(privateKey);
        signature.update(handshakeRequestTranscript(request));
        return signature.sign();
    }

    static void verifyHandshakeRequest(AeronSecureHandshakeRequestCodec.Request request,
                                       X509Certificate certificate,
                                       long nowMillis) throws GeneralSecurityException {
        if (request.createdAtMillis() > nowMillis + CLOCK_SKEW_MILLIS || request.expiresAtMillis() < nowMillis - CLOCK_SKEW_MILLIS) {
            throw new GeneralSecurityException("secure handshake request timestamp is outside the allowed window");
        }
        Signature signature = Signature.getInstance(TransportSecurityLoader.signatureAlgorithm(certificate.getPublicKey()));
        signature.initVerify(certificate.getPublicKey());
        signature.update(handshakeRequestTranscript(request));
        if (!signature.verify(request.signature())) {
            throw new GeneralSecurityException("secure handshake request signature verification failed");
        }
    }

    static byte[] signHandshakeResponse(AeronSecureHandshakeResponseCodec.Response response,
                                        AeronSecureHandshakeRequestCodec.Request request,
                                        PrivateKey privateKey,
                                        PublicKey certificateKey) throws GeneralSecurityException {
        Signature signature = Signature.getInstance(TransportSecurityLoader.signatureAlgorithm(certificateKey));
        signature.initSign(privateKey);
        signature.update(handshakeResponseTranscript(response, request));
        return signature.sign();
    }

    static void verifyHandshakeResponse(AeronSecureHandshakeResponseCodec.Response response,
                                        AeronSecureHandshakeRequestCodec.Request request,
                                        X509Certificate certificate,
                                        long nowMillis) throws GeneralSecurityException {
        if (!response.accepted()) {
            throw new GeneralSecurityException("secure handshake rejected: " + response.errorMessage());
        }
        if (response.createdAtMillis() > nowMillis + CLOCK_SKEW_MILLIS || response.expiresAtMillis() < nowMillis - CLOCK_SKEW_MILLIS) {
            throw new GeneralSecurityException("secure handshake response timestamp is outside the allowed window");
        }
        Signature signature = Signature.getInstance(TransportSecurityLoader.signatureAlgorithm(certificate.getPublicKey()));
        signature.initVerify(certificate.getPublicKey());
        signature.update(handshakeResponseTranscript(response, request));
        if (!signature.verify(response.signature())) {
            throw new GeneralSecurityException("secure handshake response signature verification failed");
        }
    }

    static SecretKey deriveSessionKey(PrivateKey localEphemeralPrivateKey,
                                      PublicKey remoteEphemeralPublicKey,
                                      byte[] clientNonce,
                                      byte[] serverNonce,
                                      long sessionId) throws GeneralSecurityException {
        KeyAgreement agreement = KeyAgreement.getInstance("X25519");
        agreement.init(localEphemeralPrivateKey);
        agreement.doPhase(remoteEphemeralPublicKey, true);
        byte[] sharedSecret = agreement.generateSecret();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(sharedSecret);
        digest.update(clientNonce);
        digest.update(serverNonce);
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(sessionId).array());
        return new SecretKeySpec(digest.digest(), 0, AES_KEY_BYTES, "AES");
    }

    static byte[] encrypt(AeronSecureSession session, int messageType, byte[] plaintext) throws GeneralSecurityException {
        long counter = session.nextOutgoingCounter(messageType);
        return encrypt(session, messageType, counter, plaintext);
    }

    static byte[] encrypt(AeronSecureSession session, int messageType, long counter, byte[] plaintext) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, session.key(), new GCMParameterSpec(GCM_TAG_BITS, nonce(session.outgoingNoncePrefix(), counter)));
        cipher.updateAAD(aad(messageType, session.sessionId(), counter));
        return cipher.doFinal(plaintext);
    }

    static byte[] decrypt(AeronSecureSession session,
                          int messageType,
                          long counter,
                          byte[] ciphertext) throws GeneralSecurityException {
        session.validateIncomingCounter(messageType, counter);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, session.key(), new GCMParameterSpec(GCM_TAG_BITS, nonce(session.incomingNoncePrefix(), counter)));
        cipher.updateAAD(aad(messageType, session.sessionId(), counter));
        return cipher.doFinal(ciphertext);
    }

    static AeronSecureSession newClientSession(String localNodeId,
                                               String remoteNodeId,
                                               TransportSecurityMaterials materials,
                                               PublicKey remoteEphemeralPublicKey,
                                               KeyPair localEphemeralKeyPair,
                                               byte[] clientNonce,
                                               byte[] serverNonce,
                                               long sessionId,
                                               long expiresAtMillis) throws GeneralSecurityException {
        SecretKey key = deriveSessionKey(localEphemeralKeyPair.getPrivate(), remoteEphemeralPublicKey, clientNonce, serverNonce, sessionId);
        return new AeronSecureSession(
                sessionId,
                localNodeId,
                remoteNodeId,
                key,
                CLIENT_TO_SERVER_NONCE_PREFIX,
                SERVER_TO_CLIENT_NONCE_PREFIX,
                materials.generation(),
                expiresAtMillis
        );
    }

    static AeronSecureSession newServerSession(String localNodeId,
                                               String remoteNodeId,
                                               TransportSecurityMaterials materials,
                                               KeyPair localEphemeralKeyPair,
                                               PublicKey remoteEphemeralPublicKey,
                                               byte[] clientNonce,
                                               byte[] serverNonce,
                                               long sessionId,
                                               long expiresAtMillis) throws GeneralSecurityException {
        SecretKey key = deriveSessionKey(localEphemeralKeyPair.getPrivate(), remoteEphemeralPublicKey, clientNonce, serverNonce, sessionId);
        return new AeronSecureSession(
                sessionId,
                localNodeId,
                remoteNodeId,
                key,
                SERVER_TO_CLIENT_NONCE_PREFIX,
                CLIENT_TO_SERVER_NONCE_PREFIX,
                materials.generation(),
                expiresAtMillis
        );
    }

    static byte[] randomBytes(SecureRandom random, int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    static byte[] handshakeRequestTranscript(AeronSecureHandshakeRequestCodec.Request request) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writeLong(out, request.requestId());
            writeLong(out, request.sessionId());
            writeString(out, request.nodeId());
            writeString(out, request.responseChannel());
            writeInt(out, request.responseStreamId());
            writeLong(out, request.createdAtMillis());
            writeLong(out, request.expiresAtMillis());
            writeBytes(out, request.clientNonce());
            writeBytes(out, request.clientEphemeralPublicKey());
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("unexpected transcript encoding failure", e);
        }
    }

    static byte[] handshakeResponseTranscript(AeronSecureHandshakeResponseCodec.Response response,
                                              AeronSecureHandshakeRequestCodec.Request request) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(handshakeRequestTranscript(request));
            writeLong(out, response.requestId());
            writeLong(out, response.sessionId());
            writeString(out, response.nodeId());
            writeLong(out, response.createdAtMillis());
            writeLong(out, response.expiresAtMillis());
            writeBytes(out, response.serverNonce());
            writeBytes(out, response.serverEphemeralPublicKey());
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("unexpected transcript encoding failure", e);
        }
    }

    private static byte[] nonce(int prefix, long counter) {
        ByteBuffer buffer = ByteBuffer.allocate(NONCE_BYTES);
        buffer.putInt(prefix);
        buffer.putLong(counter);
        return buffer.array();
    }

    private static byte[] aad(int messageType, long sessionId, long counter) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES);
        buffer.putInt(1);
        buffer.putInt(messageType);
        buffer.putLong(sessionId);
        buffer.putLong(counter);
        return buffer.array();
    }

    private static void writeLong(ByteArrayOutputStream out, long value) throws IOException {
        out.write(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static void writeInt(ByteArrayOutputStream out, int value) throws IOException {
        out.write(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static void writeString(ByteArrayOutputStream out, String value) throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(ByteArrayOutputStream out, byte[] bytes) throws IOException {
        writeInt(out, bytes.length);
        out.write(bytes);
    }

    static final class AeronSecureSession {
        private final long sessionId;
        private final String localNodeId;
        private final String remoteNodeId;
        private final SecretKey key;
        private final int outgoingNoncePrefix;
        private final int incomingNoncePrefix;
        private final long generation;
        private final long expiresAtMillis;
        private final AtomicLong[] outgoingCounters = new AtomicLong[MessageType.MAX_VALUE + 1];
        private final long[] lastIncomingCounters = new long[MessageType.MAX_VALUE + 1];

        AeronSecureSession(long sessionId,
                           String localNodeId,
                           String remoteNodeId,
                           SecretKey key,
                           int outgoingNoncePrefix,
                           int incomingNoncePrefix,
                           long generation,
                           long expiresAtMillis) {
            this.sessionId = sessionId;
            this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
            this.remoteNodeId = Objects.requireNonNull(remoteNodeId, "remoteNodeId");
            this.key = Objects.requireNonNull(key, "key");
            this.outgoingNoncePrefix = outgoingNoncePrefix;
            this.incomingNoncePrefix = incomingNoncePrefix;
            this.generation = generation;
            this.expiresAtMillis = expiresAtMillis;
            for (int type = 0; type < outgoingCounters.length; type++) {
                outgoingCounters[type] = new AtomicLong();
            }
        }

        long sessionId() {
            return sessionId;
        }

        SecretKey key() {
            return key;
        }

        int outgoingNoncePrefix() {
            return outgoingNoncePrefix;
        }

        int incomingNoncePrefix() {
            return incomingNoncePrefix;
        }

        long generation() {
            return generation;
        }

        boolean expired(long nowMillis) {
            return nowMillis >= expiresAtMillis;
        }

        long nextOutgoingCounter(int messageType) {
            validateMessageType(messageType);
            return outgoingCounters[messageType].incrementAndGet();
        }

        synchronized void validateIncomingCounter(int messageType, long counter) throws GeneralSecurityException {
            validateMessageType(messageType);
            long lastIncomingCounter = lastIncomingCounters[messageType];
            if (counter <= lastIncomingCounter) {
                throw new GeneralSecurityException("replayed or out-of-order secure Aeron message for session " + sessionId);
            }
            lastIncomingCounters[messageType] = counter;
        }

        private static void validateMessageType(int messageType) {
            if (messageType <= 0 || messageType > MessageType.MAX_VALUE) {
                throw new IllegalArgumentException("unsupported secure Aeron message type " + messageType);
            }
        }
    }

    static final class MessageType {
        static final int COMMAND = 1;
        static final int SNAPSHOT_REQUEST = 2;
        static final int SNAPSHOT_CHUNK = 3;
        static final int CONTROL_REQUEST = 4;
        static final int NODE_CONTROL_STATE = 5;
        static final int COMMAND_ACK = 6;
        static final int COMMAND_BATCH = 7;
        static final int MAX_VALUE = COMMAND_BATCH;

        private MessageType() {
        }
    }
}
