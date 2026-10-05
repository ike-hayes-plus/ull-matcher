package io.github.ike.ullmatcher.ha.etcd;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

final class EtcdTlsSupport {
    private static final String PKCS8_BEGIN = "-----BEGIN PRIVATE KEY-----";
    private static final String PKCS8_END = "-----END PRIVATE KEY-----";
    private static final List<String> LEGACY_PEM_HEADERS =
            List.of("-----BEGIN RSA PRIVATE KEY-----", "-----BEGIN EC PRIVATE KEY-----", "-----BEGIN DSA PRIVATE KEY-----");
    private static final List<String> PKCS8_KEY_ALGORITHMS = List.of("RSA", "EC", "Ed25519");

    private EtcdTlsSupport() {}

    static SSLContext sslContext(Path trustChainFile, Path certificateChainFile, Path privateKeyFile) throws IOException {
        try {
            TrustManagerFactory trustManagers = trustManagers(trustChainFile);
            KeyManagerFactory keyManagers = keyManagers(certificateChainFile, privateKeyFile);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(
                    keyManagers == null ? null : keyManagers.getKeyManagers(),
                    trustManagers == null ? null : trustManagers.getTrustManagers(),
                    null
            );
            return context;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("failed to build etcd TLS context", e);
        }
    }

    private static TrustManagerFactory trustManagers(Path trustChainFile) throws Exception {
        if (trustChainFile == null) {
            return null;
        }
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        Certificate[] certificates = readCertificates(trustChainFile);
        for (int i = 0; i < certificates.length; i++) {
            trustStore.setCertificateEntry("etcd-trust-" + i, certificates[i]);
        }
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(trustStore);
        return factory;
    }

    private static KeyManagerFactory keyManagers(Path certificateChainFile, Path privateKeyFile) throws Exception {
        if (certificateChainFile == null && privateKeyFile == null) {
            return null;
        }
        if (certificateChainFile == null || privateKeyFile == null) {
            throw new IOException("etcd mTLS requires both certificate chain and private key");
        }
        Certificate[] certificates = readCertificates(certificateChainFile);
        PrivateKey privateKey = readPrivateKey(privateKeyFile);
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry("etcd-client", privateKey, new char[0], certificates);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(keyStore, new char[0]);
        return factory;
    }

    private static Certificate[] readCertificates(Path file) throws IOException {
        final CertificateFactory factory;
        try {
            factory = CertificateFactory.getInstance("X.509");
        } catch (Exception e) {
            throw new IOException("failed to initialize certificate factory", e);
        }
        try (InputStream input = Files.newInputStream(file)) {
            List<Certificate> certificates = new ArrayList<>();
            for (Certificate certificate : factory.generateCertificates(input)) {
                certificates.add(certificate);
            }
            if (certificates.isEmpty()) {
                throw new IOException("certificate file is empty: " + file);
            }
            return certificates.toArray(Certificate[]::new);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("failed to read certificate file " + file, e);
        }
    }

    private static PrivateKey readPrivateKey(Path file) throws IOException {
        String pem = Files.readString(file);
        if (pem.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----")) {
            throw new IOException("encrypted private keys are not supported: " + file
                    + "; decrypt it first with: openssl pkcs8 -topk8 -nocrypt -in " + file + " -out decrypted-pkcs8.pem");
        }
        for (String legacyHeader : LEGACY_PEM_HEADERS) {
            if (pem.contains(legacyHeader)) {
                throw new IOException("unsupported PKCS#1/SEC1 private key format in " + file
                        + "; convert it to PKCS#8 with: openssl pkcs8 -topk8 -nocrypt -in " + file + " -out pkcs8.pem");
            }
        }
        int begin = pem.indexOf(PKCS8_BEGIN);
        int end = pem.indexOf(PKCS8_END);
        if (begin < 0 || end < begin) {
            throw new IOException("private key file does not contain a PKCS#8 PEM block: " + file);
        }
        String body = pem.substring(begin + PKCS8_BEGIN.length(), end).replaceAll("\\s", "");
        final byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(body);
        } catch (IllegalArgumentException e) {
            throw new IOException("private key file is not valid base64: " + file, e);
        }
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(decoded);
        for (String algorithm : PKCS8_KEY_ALGORITHMS) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (GeneralSecurityException ignored) {
                // try the next algorithm; the key type is not encoded in the PEM header
            }
        }
        throw new IOException("unsupported private key algorithm in " + file
                + "; expected one of " + String.join(", ", PKCS8_KEY_ALGORITHMS));
    }
}
