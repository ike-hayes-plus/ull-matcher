package io.github.ike.ullmatcher.server.cluster;

import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class AeronTransportSecuritySupportTest {
    @Test
    void secureSessionTracksIncomingCountersPerMessageType() throws Exception {
        AeronTransportSecuritySupport.AeronSecureSession session = session(7L);

        byte[] controlPlaintext = "control".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] snapshotPlaintext = "snapshot".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        byte[] controlCiphertext = AeronTransportSecuritySupport.encrypt(
                session,
                AeronTransportSecuritySupport.MessageType.NODE_CONTROL_STATE,
                2L,
                controlPlaintext
        );
        byte[] snapshotCiphertext = AeronTransportSecuritySupport.encrypt(
                session,
                AeronTransportSecuritySupport.MessageType.SNAPSHOT_CHUNK,
                1L,
                snapshotPlaintext
        );

        assertArrayEquals(
                controlPlaintext,
                AeronTransportSecuritySupport.decrypt(
                        session,
                        AeronTransportSecuritySupport.MessageType.NODE_CONTROL_STATE,
                        2L,
                        controlCiphertext
                )
        );
        assertArrayEquals(
                snapshotPlaintext,
                AeronTransportSecuritySupport.decrypt(
                        session,
                        AeronTransportSecuritySupport.MessageType.SNAPSHOT_CHUNK,
                        1L,
                        snapshotCiphertext
                )
        );
    }

    @Test
    void secureSessionRejectsReplayWithinSameMessageType() throws Exception {
        AeronTransportSecuritySupport.AeronSecureSession session = session(9L);
        byte[] plaintext = "control".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] ciphertext = AeronTransportSecuritySupport.encrypt(
                session,
                AeronTransportSecuritySupport.MessageType.NODE_CONTROL_STATE,
                1L,
                plaintext
        );

        AeronTransportSecuritySupport.decrypt(
                session,
                AeronTransportSecuritySupport.MessageType.NODE_CONTROL_STATE,
                1L,
                ciphertext
        );

        assertThrows(
                GeneralSecurityException.class,
                () -> AeronTransportSecuritySupport.decrypt(
                        session,
                        AeronTransportSecuritySupport.MessageType.NODE_CONTROL_STATE,
                        1L,
                        ciphertext
                )
        );
    }

    private static AeronTransportSecuritySupport.AeronSecureSession session(long sessionId) {
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        return new AeronTransportSecuritySupport.AeronSecureSession(
                sessionId,
                "local",
                "remote",
                key,
                0x53525643,
                0x53525643,
                1L,
                Long.MAX_VALUE
        );
    }
}
