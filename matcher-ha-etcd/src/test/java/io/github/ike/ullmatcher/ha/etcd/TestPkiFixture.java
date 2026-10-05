package io.github.ike.ullmatcher.ha.etcd;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Generates a throwaway self-signed certificate and PKCS#8 key pair for TLS tests.
 * <p>
 * The JDK ships {@code keytool}, so no external tooling or crypto provider is required.
 */
final class TestPkiFixture {
    private static final char[] PASSWORD = "changeit".toCharArray();

    private final Path keyStoreFile;
    private final Path certificatePem;
    private final Path privateKeyPem;

    private TestPkiFixture(Path keyStoreFile, Path certificatePem, Path privateKeyPem) {
        this.keyStoreFile = keyStoreFile;
        this.certificatePem = certificatePem;
        this.privateKeyPem = privateKeyPem;
    }

    static TestPkiFixture create(Path directory, String keyAlgorithm) throws Exception {
        Files.createDirectories(directory);
        Path keyStoreFile = directory.resolve("keystore.p12");
        runKeytool(
                "-genkeypair",
                "-alias", "test",
                "-keyalg", keyAlgorithm,
                "-keysize", "RSA".equals(keyAlgorithm) ? "2048" : "256",
                "-sigalg", "RSA".equals(keyAlgorithm) ? "SHA256withRSA" : "SHA256withECDSA",
                "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-validity", "1",
                "-storetype", "PKCS12",
                "-keystore", keyStoreFile.toString(),
                "-storepass", new String(PASSWORD)
        );

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(keyStoreFile)) {
            keyStore.load(input, PASSWORD);
        }
        Certificate certificate = keyStore.getCertificate("test");
        PrivateKey privateKey = (PrivateKey) keyStore.getKey("test", PASSWORD);

        Path certificatePem = directory.resolve("cert.pem");
        Files.writeString(certificatePem, pem("CERTIFICATE", certificate.getEncoded()), StandardCharsets.US_ASCII);
        Path privateKeyPem = directory.resolve("key-pkcs8.pem");
        Files.writeString(privateKeyPem, pem("PRIVATE KEY", privateKey.getEncoded()), StandardCharsets.US_ASCII);

        return new TestPkiFixture(keyStoreFile, certificatePem, privateKeyPem);
    }

    Path keyStoreFile() {
        return keyStoreFile;
    }

    char[] keyStorePassword() {
        return PASSWORD.clone();
    }

    Path certificatePem() {
        return certificatePem;
    }

    Path privateKeyPem() {
        return privateKeyPem;
    }

    static String pem(String type, byte[] der) {
        StringBuilder builder = new StringBuilder("-----BEGIN ").append(type).append("-----\n");
        String encoded = Base64.getEncoder().encodeToString(der);
        for (int offset = 0; offset < encoded.length(); offset += 64) {
            builder.append(encoded, offset, Math.min(offset + 64, encoded.length())).append('\n');
        }
        return builder.append("-----END ").append(type).append("-----\n").toString();
    }

    private static void runKeytool(String... arguments) throws IOException, InterruptedException {
        Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
        String[] command = new String[arguments.length + 1];
        command[0] = keytool.toString();
        System.arraycopy(arguments, 0, command, 1, arguments.length);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output;
        try (InputStream input = process.getInputStream()) {
            output = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        if (!process.waitFor(60L, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IOException("keytool timed out");
        }
        if (process.exitValue() != 0) {
            throw new IOException("keytool failed with exit code " + process.exitValue() + ": " + output);
        }
    }
}
