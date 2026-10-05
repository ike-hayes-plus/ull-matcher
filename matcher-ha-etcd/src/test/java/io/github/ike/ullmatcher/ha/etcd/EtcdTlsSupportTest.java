package io.github.ike.ullmatcher.ha.etcd;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class EtcdTlsSupportTest {
    @Test
    void buildsContextFromRsaCertificateAndPkcs8Key(@TempDir Path directory) throws Exception {
        TestPkiFixture pki = TestPkiFixture.create(directory, "RSA");

        SSLContext context = EtcdTlsSupport.sslContext(pki.certificatePem(), pki.certificatePem(), pki.privateKeyPem());

        assertNotNull(context);
        assertNotNull(context.getSocketFactory());
    }

    @Test
    void buildsContextFromEcCertificateAndPkcs8Key(@TempDir Path directory) throws Exception {
        TestPkiFixture pki = TestPkiFixture.create(directory, "EC");

        SSLContext context = EtcdTlsSupport.sslContext(pki.certificatePem(), pki.certificatePem(), pki.privateKeyPem());

        assertNotNull(context);
    }

    @Test
    void buildsTrustOnlyContextWithoutClientCredentials(@TempDir Path directory) throws Exception {
        TestPkiFixture pki = TestPkiFixture.create(directory, "RSA");

        assertNotNull(EtcdTlsSupport.sslContext(pki.certificatePem(), null, null));
    }

    @Test
    void acceptsEd25519Pkcs8Key(@TempDir Path directory) throws Exception {
        PrivateKey key = KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate();
        Path keyFile = directory.resolve("ed25519.pem");
        Files.writeString(keyFile, TestPkiFixture.pem("PRIVATE KEY", key.getEncoded()), StandardCharsets.US_ASCII);
        TestPkiFixture pki = TestPkiFixture.create(directory.resolve("ca"), "RSA");

        // Proves the Ed25519 KeyFactory fallback is reached; a PKCS#8-RSA-only parser would fail here.
        assertNotNull(EtcdTlsSupport.sslContext(pki.certificatePem(), pki.certificatePem(), keyFile));
    }

    @Test
    void rejectsPkcs1KeyWithConversionHint(@TempDir Path directory) throws Exception {
        Path keyFile = directory.resolve("pkcs1.pem");
        Files.writeString(keyFile, "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n");
        TestPkiFixture pki = TestPkiFixture.create(directory.resolve("ca"), "RSA");

        IOException error = assertThrows(IOException.class,
                () -> EtcdTlsSupport.sslContext(null, pki.certificatePem(), keyFile));

        assertTrue(error.getMessage().contains("PKCS#1"), error.getMessage());
        assertTrue(error.getMessage().contains("openssl pkcs8 -topk8"), error.getMessage());
    }

    @Test
    void rejectsSec1EcKeyWithConversionHint(@TempDir Path directory) throws Exception {
        Path keyFile = directory.resolve("sec1.pem");
        Files.writeString(keyFile, "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n");
        TestPkiFixture pki = TestPkiFixture.create(directory.resolve("ca"), "RSA");

        IOException error = assertThrows(IOException.class,
                () -> EtcdTlsSupport.sslContext(null, pki.certificatePem(), keyFile));

        assertTrue(error.getMessage().contains("openssl pkcs8 -topk8"), error.getMessage());
    }

    @Test
    void rejectsEncryptedPrivateKey(@TempDir Path directory) throws Exception {
        Path keyFile = directory.resolve("encrypted.pem");
        Files.writeString(keyFile, "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n");
        TestPkiFixture pki = TestPkiFixture.create(directory.resolve("ca"), "RSA");

        IOException error = assertThrows(IOException.class,
                () -> EtcdTlsSupport.sslContext(null, pki.certificatePem(), keyFile));

        assertTrue(error.getMessage().contains("encrypted private keys are not supported"), error.getMessage());
    }

    @Test
    void rejectsKeyFileWithoutPemBlock(@TempDir Path directory) throws Exception {
        Path keyFile = directory.resolve("garbage.pem");
        Files.writeString(keyFile, "not a pem file\n");
        TestPkiFixture pki = TestPkiFixture.create(directory.resolve("ca"), "RSA");

        IOException error = assertThrows(IOException.class,
                () -> EtcdTlsSupport.sslContext(null, pki.certificatePem(), keyFile));

        assertTrue(error.getMessage().contains("PKCS#8 PEM block"), error.getMessage());
    }

    @Test
    void rejectsNonBase64PemBody(@TempDir Path directory) throws Exception {
        Path keyFile = directory.resolve("bad-base64.pem");
        Files.writeString(keyFile, "-----BEGIN PRIVATE KEY-----\n!!!!\n-----END PRIVATE KEY-----\n");
        TestPkiFixture pki = TestPkiFixture.create(directory.resolve("ca"), "RSA");

        IOException error = assertThrows(IOException.class,
                () -> EtcdTlsSupport.sslContext(null, pki.certificatePem(), keyFile));

        assertTrue(error.getMessage().contains("not valid base64"), error.getMessage());
    }

    @Test
    void rejectsEmptyCertificateFile(@TempDir Path directory) throws Exception {
        Path certificate = directory.resolve("empty.pem");
        Files.writeString(certificate, "");

        assertThrows(IOException.class, () -> EtcdTlsSupport.sslContext(certificate, null, null));
    }

    @Test
    void rejectsCertificateWithoutPrivateKey(@TempDir Path directory) throws Exception {
        TestPkiFixture pki = TestPkiFixture.create(directory, "RSA");

        IOException error = assertThrows(IOException.class,
                () -> EtcdTlsSupport.sslContext(null, pki.certificatePem(), null));

        assertTrue(error.getMessage().contains("requires both"), error.getMessage());
    }

    @Test
    void returnsDefaultContextWhenNothingIsConfigured() throws Exception {
        assertNotNull(EtcdTlsSupport.sslContext(null, null, null));
    }
}
