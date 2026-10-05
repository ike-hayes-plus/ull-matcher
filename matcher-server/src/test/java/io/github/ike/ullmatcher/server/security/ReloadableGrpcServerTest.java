package io.github.ike.ullmatcher.server.security;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationService;
import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.transport.TransportSecuritySnapshot;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ReloadableGrpcServerTest {
    @Test
    void plaintextServerExposesAStableGenerationAndNoReloadThread() throws Exception {
        try (ReloadableGrpcServer server = new ReloadableGrpcServer(
                () -> GrpcReplicationServerConfig.defaults(0),
                ReloadableGrpcServerTest::replicationService,
                ServerSecurityConfig.insecureDefaults())) {
            server.start();

            assertTrue(server.port() > 0);

            TransportSecuritySnapshot snapshot = server.snapshot();
            assertEquals(1L, snapshot.generation());
            assertEquals(0L, snapshot.reloadCount());
            assertEquals(0L, snapshot.failureCount());
            assertFalse(snapshot.reloading());
            assertEquals("", snapshot.lastError());
        }
    }

    @Test
    void collaboratorsAreRequired() {
        Supplier<GrpcReplicationServerConfig> configSupplier = () -> GrpcReplicationServerConfig.defaults(0);

        assertEquals("configSupplier", assertThrows(NullPointerException.class, () -> new ReloadableGrpcServer(
                null, ReloadableGrpcServerTest::replicationService, ServerSecurityConfig.insecureDefaults())).getMessage());
        assertEquals("serviceSupplier", assertThrows(NullPointerException.class, () -> new ReloadableGrpcServer(
                configSupplier, null, ServerSecurityConfig.insecureDefaults())).getMessage());
        assertEquals("securityConfig", assertThrows(NullPointerException.class, () -> new ReloadableGrpcServer(
                configSupplier, ReloadableGrpcServerTest::replicationService, null)).getMessage());
    }

    @Test
    void rotatedCertificatesAreSwappedInWithoutRestartingTheProcess() throws Exception {
        Path dir = Files.createTempDirectory("grpc-tls-reload");
        CertificateAuthority authority = CertificateAuthority.create("CN=ULL gRPC Reload CA");
        IdentityMaterial initial = authority.issue("CN=node-a", dir.resolve("node-a"));
        Path trustChain = dir.resolve("ca.crt");
        writePem(trustChain, authority.certificate());
        ServerSecurityConfig securityConfig = ServerSecurityConfig.fromPaths(
                initial.certificatePath(), initial.privateKeyPath(), trustChain, false, 100L, false);

        try (ReloadableGrpcServer server = new ReloadableGrpcServer(
                () -> GrpcReplicationServerConfig.defaults(0).withBindHost("127.0.0.1"),
                ReloadableGrpcServerTest::replicationService,
                securityConfig)) {
            server.start();
            int originalPort = server.port();
            assertTrue(originalPort > 0);

            IdentityMaterial rotated = authority.issue("CN=node-a-rotated", dir.resolve("node-a-rotated"));
            Files.copy(rotated.certificatePath(), initial.certificatePath(), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(rotated.privateKeyPath(), initial.privateKeyPath(), StandardCopyOption.REPLACE_EXISTING);

            assertTrue(await(() -> server.snapshot().reloadCount() > 0L, 15_000L),
                    "expected a TLS reload, snapshot=" + server.snapshot());
            assertTrue(server.snapshot().generation() > 1L);
            assertEquals("", server.snapshot().lastError());
            assertFalse(server.snapshot().reloading());
        }
    }

    @Test
    void unusableRotatedMaterialIsCountedAsAReloadFailure() throws Exception {
        Path dir = Files.createTempDirectory("grpc-tls-reload-failure");
        CertificateAuthority authority = CertificateAuthority.create("CN=ULL gRPC Failure CA");
        IdentityMaterial initial = authority.issue("CN=node-a", dir.resolve("node-a"));
        Path trustChain = dir.resolve("ca.crt");
        writePem(trustChain, authority.certificate());
        ServerSecurityConfig securityConfig = ServerSecurityConfig.fromPaths(
                initial.certificatePath(), initial.privateKeyPath(), trustChain, false, 100L, false);

        try (ReloadableGrpcServer server = new ReloadableGrpcServer(
                () -> GrpcReplicationServerConfig.defaults(0).withBindHost("127.0.0.1"),
                ReloadableGrpcServerTest::replicationService,
                securityConfig)) {
            server.start();

            Files.writeString(initial.certificatePath(), "-----BEGIN CERTIFICATE-----\nnot-a-certificate\n", StandardCharsets.UTF_8);
            // WatchService on macOS can coalesce the first write; a second mutation forces a new event.
            Files.writeString(initial.certificatePath(), "-----BEGIN CERTIFICATE-----\nstill-not-a-certificate\n", StandardCharsets.UTF_8);
            Files.writeString(initial.privateKeyPath(), Files.readString(initial.privateKeyPath()) + "\n", StandardCharsets.UTF_8);

            assertTrue(await(() -> server.snapshot().failureCount() > 0L, 15_000L),
                    "expected a TLS reload failure, snapshot=" + server.snapshot());
            assertFalse(server.snapshot().lastError().isBlank());
            assertEquals(1L, server.snapshot().generation());
        }
    }

    private static GrpcReplicationService replicationService() {
        return new GrpcReplicationService(
                () -> null,
                () -> new NodeControlState(
                        "node-a",
                        HaRole.PRIMARY,
                        new FencingToken(1L),
                        false,
                        MatchLoopState.RUNNING,
                        0L,
                        new ReplicationCursor(0L, 0L, 0L, 0L)
                ),
                () -> new SnapshotMaterial(Files.createTempFile("grpc-reload-snapshot", ".snap"), 0L, 0L, 0L),
                new GrpcTransportMetrics(),
                TimeUnit.SECONDS.toNanos(1)
        );
    }

    private static boolean await(Check check, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (check.ok()) {
                return true;
            }
            Thread.sleep(25L);
        }
        return check.ok();
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    private record IdentityMaterial(Path certificatePath, Path privateKeyPath) {
    }

    private record CertificateAuthority(X509Certificate certificate, KeyPair keyPair) {
        private static CertificateAuthority create(String subject) throws Exception {
            KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
            X500Name issuer = new X500Name(subject);
            Instant now = Instant.now();
            X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                    issuer,
                    BigInteger.valueOf(System.nanoTime()),
                    Date.from(now.minusSeconds(60)),
                    Date.from(now.plusSeconds(TimeUnit.DAYS.toSeconds(1))),
                    issuer,
                    keyPair.getPublic()
            );
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
            ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());
            return new CertificateAuthority(
                    new JcaX509CertificateConverter().getCertificate(builder.build(signer)), keyPair);
        }

        private IdentityMaterial issue(String subject, Path prefix) throws Exception {
            KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
            Instant now = Instant.now();
            X500Name issuer = new X500Name(certificate.getSubjectX500Principal().getName());
            X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                    issuer,
                    BigInteger.valueOf(System.nanoTime()),
                    Date.from(now.minusSeconds(60)),
                    Date.from(now.plusSeconds(TimeUnit.DAYS.toSeconds(1))),
                    new X500Name(subject),
                    keyPair.getPublic()
            );
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
            builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
            ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair().getPrivate());
            X509Certificate leaf = new JcaX509CertificateConverter().getCertificate(builder.build(signer));
            Path certPath = prefix.resolveSibling(prefix.getFileName() + ".crt");
            Path keyPath = prefix.resolveSibling(prefix.getFileName() + ".key");
            writePem(certPath, leaf);
            writePem(keyPath, keyPair.getPrivate());
            return new IdentityMaterial(certPath, keyPath);
        }
    }

    private static void writePem(Path path, Object entry) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
             JcaPEMWriter pem = new JcaPEMWriter(writer)) {
            pem.writeObject(entry);
        }
        assertTrue(Files.size(path) > 0L);
    }
}
