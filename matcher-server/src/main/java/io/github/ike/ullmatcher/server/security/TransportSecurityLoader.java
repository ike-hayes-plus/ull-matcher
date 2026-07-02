package io.github.ike.ullmatcher.server.security;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 负责从磁盘加载传输证书、私钥与信任链，并校验证书用途。
 */
public final class TransportSecurityLoader {
    private static final JcaPEMKeyConverter KEY_CONVERTER = new JcaPEMKeyConverter();
    private static final JcaX509CertificateConverter CERTIFICATE_CONVERTER = new JcaX509CertificateConverter();
    private static final byte[] SELF_TEST_MESSAGE = "ull-matcher-transport-self-test".getBytes(StandardCharsets.UTF_8);

    private TransportSecurityLoader() {
    }

    public static TransportSecurityMaterials load(TransportSecurityConfig config, long generation) throws IOException, GeneralSecurityException {
        Objects.requireNonNull(config, "config");
        List<X509Certificate> certificateChain = loadCertificates(config.certificateChainFile());
        if (certificateChain.isEmpty()) {
            throw new GeneralSecurityException("transport certificate chain is empty");
        }
        PrivateKey privateKey = loadPrivateKey(config.privateKeyFile());
        assertKeyMatchesCertificate(privateKey, certificateChain.getFirst().getPublicKey());
        Set<TrustAnchor> trustAnchors = loadTrustAnchors(config.trustCertCollectionFile());
        if (config.requireMutualTls() && trustAnchors.isEmpty()) {
            throw new GeneralSecurityException("mutual transport authentication requires trusted CA certificates");
        }
        List<byte[]> encodedChain = new ArrayList<>(certificateChain.size());
        for (X509Certificate certificate : certificateChain) {
            encodedChain.add(certificate.getEncoded());
        }
        return new TransportSecurityMaterials(
                generation,
                certificateChain.getFirst(),
                List.copyOf(certificateChain),
                List.copyOf(encodedChain),
                privateKey,
                Set.copyOf(trustAnchors),
                fingerprint(certificateChain.getFirst())
        );
    }

    public static List<X509Certificate> decodeCertificateChain(List<byte[]> derChain) throws GeneralSecurityException {
        List<X509Certificate> certificates = new ArrayList<>(derChain.size());
        try {
            for (byte[] der : derChain) {
                certificates.add(CERTIFICATE_CONVERTER.getCertificate(new X509CertificateHolder(der)));
            }
        } catch (IOException | CertificateException e) {
            throw new GeneralSecurityException("failed to decode peer certificate chain", e);
        }
        return List.copyOf(certificates);
    }

    public static void validatePeerCertificates(List<X509Certificate> peerChain, Set<TrustAnchor> trustAnchors)
            throws GeneralSecurityException {
        if (peerChain.isEmpty()) {
            throw new GeneralSecurityException("peer certificate chain is empty");
        }
        if (trustAnchors.isEmpty()) {
            throw new GeneralSecurityException("no trusted certificate authorities configured for peer validation");
        }
        CertPath certPath = java.security.cert.CertificateFactory.getInstance("X.509").generateCertPath(peerChain);
        PKIXParameters parameters = new PKIXParameters(trustAnchors);
        parameters.setRevocationEnabled(false);
        CertPathValidator.getInstance("PKIX").validate(certPath, parameters);
        for (X509Certificate certificate : peerChain) {
            certificate.checkValidity();
        }
    }

    public static String signatureAlgorithm(PublicKey publicKey) throws GeneralSecurityException {
        return switch (publicKey.getAlgorithm()) {
            case "RSA" -> "SHA256withRSA";
            case "EC" -> "SHA256withECDSA";
            case "Ed25519" -> "Ed25519";
            default -> throw new GeneralSecurityException("unsupported certificate key algorithm " + publicKey.getAlgorithm());
        };
    }

    private static List<X509Certificate> loadCertificates(java.nio.file.Path path) throws IOException, GeneralSecurityException {
        List<X509Certificate> certificates = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
             PEMParser parser = new PEMParser(reader)) {
            Object parsed;
            while ((parsed = parser.readObject()) != null) {
                if (parsed instanceof X509CertificateHolder holder) {
                    certificates.add(CERTIFICATE_CONVERTER.getCertificate(holder));
                }
            }
        } catch (CertificateException e) {
            throw new GeneralSecurityException("failed to decode transport certificate chain from " + path, e);
        }
        return List.copyOf(certificates);
    }

    private static PrivateKey loadPrivateKey(java.nio.file.Path path) throws IOException, GeneralSecurityException {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
             PEMParser parser = new PEMParser(reader)) {
            Object parsed = parser.readObject();
            if (parsed == null) {
                throw new GeneralSecurityException("transport private key file is empty");
            }
            if (parsed instanceof PrivateKeyInfo privateKeyInfo) {
                return KEY_CONVERTER.getPrivateKey(privateKeyInfo);
            }
            if (parsed instanceof PEMKeyPair pemKeyPair) {
                return KEY_CONVERTER.getKeyPair(pemKeyPair).getPrivate();
            }
            if (parsed instanceof PKCS8EncryptedPrivateKeyInfo || parsed instanceof PEMEncryptedKeyPair) {
                throw new GeneralSecurityException("encrypted transport private keys are not supported");
            }
            throw new GeneralSecurityException("unsupported PEM object in transport private key: " + parsed.getClass().getSimpleName());
        }
    }

    private static Set<TrustAnchor> loadTrustAnchors(java.nio.file.Path path) throws IOException, GeneralSecurityException {
        if (path == null) {
            return Set.of();
        }
        List<X509Certificate> certificates = loadCertificates(path);
        Set<TrustAnchor> anchors = new LinkedHashSet<>(certificates.size());
        for (X509Certificate certificate : certificates) {
            anchors.add(new TrustAnchor(certificate, null));
        }
        return Set.copyOf(anchors);
    }

    private static void assertKeyMatchesCertificate(PrivateKey privateKey, PublicKey publicKey) throws GeneralSecurityException {
        Signature signature = Signature.getInstance(signatureAlgorithm(publicKey));
        signature.initSign(privateKey);
        signature.update(SELF_TEST_MESSAGE);
        byte[] signed = signature.sign();
        signature.initVerify(publicKey);
        signature.update(SELF_TEST_MESSAGE);
        if (!signature.verify(signed)) {
            throw new GeneralSecurityException("transport private key does not match the certificate public key");
        }
    }

    private static String fingerprint(X509Certificate certificate) throws CertificateEncodingException, GeneralSecurityException {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
        return HexFormat.of().formatHex(digest);
    }
}
