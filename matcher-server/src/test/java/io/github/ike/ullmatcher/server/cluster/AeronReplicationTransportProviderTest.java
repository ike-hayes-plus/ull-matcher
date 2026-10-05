package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterialSource;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.state.NodeControlStateSource;
import io.github.ike.ullmatcher.ha.transport.ClusterPeerClient;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportProvider;
import io.github.ike.ullmatcher.ha.transport.ReplicationTransportType;
import io.github.ike.ullmatcher.ha.transport.TransportMetricsSnapshot;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
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
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AeronReplicationTransportProviderTest {
    @Test
    void authoritativeProviderAdvertisesEveryAeronEndpointItListensOn() throws Exception {
        Path dir = Files.createTempDirectory("aeron-provider-metadata");
        AeronPreviewTransportConfig config = new AeronPreviewTransportConfig(dir.resolve("aeron"), 21_010, 17_010);

        try (AeronReplicationTransportProvider provider = authoritativeProvider(ServerSecurityConfig.insecureDefaults(), config)) {
            assertEquals(ReplicationTransportType.AERON, provider.type());

            Map<String, String> metadata = provider.localNodeMetadata();
            assertEquals("AERON", metadata.get(ReplicationTransportProvider.TRANSPORT_METADATA_KEY));
            assertEquals(config.commandChannel("127.0.0.1"),
                    metadata.get(ReplicationTransportProvider.AERON_CHANNEL_METADATA_KEY));
            assertEquals("17010", metadata.get(ReplicationTransportProvider.AERON_STREAM_ID_METADATA_KEY));
            assertEquals(config.snapshotRequestChannel("127.0.0.1"),
                    metadata.get(ReplicationTransportProvider.AERON_SNAPSHOT_REQUEST_CHANNEL_METADATA_KEY));
            assertEquals("17110", metadata.get(ReplicationTransportProvider.AERON_SNAPSHOT_REQUEST_STREAM_ID_METADATA_KEY));
            assertEquals(config.controlRequestChannel("127.0.0.1"),
                    metadata.get(ReplicationTransportProvider.AERON_CONTROL_REQUEST_CHANNEL_METADATA_KEY));
            assertEquals("17310", metadata.get(ReplicationTransportProvider.AERON_CONTROL_REQUEST_STREAM_ID_METADATA_KEY));
            assertEquals(config.securityHandshakeRequestChannel("127.0.0.1"),
                    metadata.get(ReplicationTransportProvider.AERON_SECURITY_HANDSHAKE_REQUEST_CHANNEL_METADATA_KEY));
            assertEquals("17510",
                    metadata.get(ReplicationTransportProvider.AERON_SECURITY_HANDSHAKE_REQUEST_STREAM_ID_METADATA_KEY));

            TransportMetricsSnapshot metrics = provider.metricsSnapshot();
            assertEquals("AERON", metrics.transportType());
            assertEquals("DISABLED", metrics.reconciliationStatus());
            assertEquals("STABLE", metrics.policyStatus());
            assertEquals(0L, metrics.previewPublishedCommands());

            assertFalse(provider.securitySnapshot().reloading());
            assertEquals(0L, provider.securitySnapshot().generation());
        }
    }

    @Test
    void authoritativeProviderRejectsPeersWithoutAeronMetadata() throws Exception {
        Path dir = Files.createTempDirectory("aeron-provider-missing-metadata");
        AeronPreviewTransportConfig config = new AeronPreviewTransportConfig(dir.resolve("aeron"), 21_020, 17_020);

        try (AeronReplicationTransportProvider provider = authoritativeProvider(ServerSecurityConfig.insecureDefaults(), config)) {
            DiscoveredNode bare = new DiscoveredNode("node-b", "127.0.0.1", 1, HaRole.STANDBY, Map.of());
            assertEquals("remote node node-b does not advertise Aeron transport metadata",
                    assertThrows(IOException.class, () -> provider.connect(bare)).getMessage());

            DiscoveredNode partial = new DiscoveredNode("node-c", "127.0.0.1", 1, HaRole.STANDBY, Map.of(
                    ReplicationTransportProvider.AERON_CHANNEL_METADATA_KEY, "aeron:udp?endpoint=127.0.0.1:21021",
                    ReplicationTransportProvider.AERON_STREAM_ID_METADATA_KEY, "17021"));
            assertEquals("remote node node-c does not advertise Aeron transport metadata",
                    assertThrows(IOException.class, () -> provider.connect(partial)).getMessage());
        }
    }

    @Test
    void authoritativeProviderAllocatesDistinctResponseEndpointsPerPeer() throws Exception {
        Path dir = Files.createTempDirectory("aeron-provider-connect");
        AeronPreviewTransportConfig config = new AeronPreviewTransportConfig(dir.resolve("aeron"), 21_030, 17_030);

        try (AeronReplicationTransportProvider provider = authoritativeProvider(ServerSecurityConfig.insecureDefaults(), config)) {
            ClusterPeerClient first = provider.connect(peer("node-b", 21_031, 17_031));
            ClusterPeerClient second = provider.connect(peer("node-c", 21_041, 17_041));
            ClusterPeerClient cachedFirst = provider.connect(peer("node-b", 21_031, 17_031));
            try {
                assertEquals("node-b", first.nodeId());
                assertEquals("node-c", second.nodeId());
                assertEquals("node-b", cachedFirst.nodeId());
            } finally {
                first.close();
                second.close();
                cachedFirst.close();
            }
        }
    }

    @Test
    void authoritativeProviderPublishesTheSecurityContextSnapshotWhenTlsIsConfigured() throws Exception {
        Path dir = Files.createTempDirectory("aeron-provider-secure");
        AeronPreviewTransportConfig config = new AeronPreviewTransportConfig(dir.resolve("aeron"), 21_050, 17_050);
        ServerSecurityConfig securityConfig = secureConfig(dir);

        try (AeronReplicationTransportProvider provider = authoritativeProvider(securityConfig, config)) {
            assertEquals(1L, provider.securitySnapshot().generation());
            assertFalse(provider.securitySnapshot().reloading());
        }
    }

    @Test
    void authoritativeProviderRejectsUnusableTransportSecurityMaterial() throws Exception {
        Path dir = Files.createTempDirectory("aeron-provider-broken-tls");
        Files.writeString(dir.resolve("broken.crt"), "not a certificate\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("broken.key"), "not a key\n", StandardCharsets.UTF_8);
        ServerSecurityConfig securityConfig = ServerSecurityConfig.fromPaths(
                dir.resolve("broken.crt"), dir.resolve("broken.key"), dir.resolve("broken.crt"), false, 0L, false);
        AeronPreviewTransportConfig config = new AeronPreviewTransportConfig(dir.resolve("aeron"), 21_060, 17_060);

        assertEquals("failed to initialize Aeron transport security context",
                assertThrows(IllegalStateException.class, () -> authoritativeProvider(securityConfig, config)).getMessage());
    }

    @Test
    void previewProviderShadowsGrpcPeersThatAdvertiseAnAeronChannel() throws Exception {
        Path dir = Files.createTempDirectory("aeron-preview-provider");
        AeronPreviewTransportConfig config = new AeronPreviewTransportConfig(dir.resolve("aeron"), 21_070, 17_070);

        try (AeronPreviewReplicationTransportProvider provider = new AeronPreviewReplicationTransportProvider(
                ServerSecurityConfig.insecureDefaults(), "127.0.0.1", config, () -> 0L)) {
            assertEquals(ReplicationTransportType.AERON_PREVIEW, provider.type());

            Map<String, String> metadata = provider.localNodeMetadata();
            assertEquals("AERON_PREVIEW", metadata.get(ReplicationTransportProvider.TRANSPORT_METADATA_KEY));
            assertEquals("aeron:udp?endpoint=127.0.0.1:21070",
                    metadata.get(ReplicationTransportProvider.AERON_CHANNEL_METADATA_KEY));
            assertEquals("17070", metadata.get(ReplicationTransportProvider.AERON_STREAM_ID_METADATA_KEY));

            TransportMetricsSnapshot metrics = provider.metricsSnapshot();
            assertEquals("AERON_PREVIEW", metrics.transportType());
            assertEquals("IDLE", metrics.reconciliationStatus());

            DiscoveredNode grpcOnly = new DiscoveredNode("node-b", "127.0.0.1", 1, HaRole.STANDBY, Map.of());
            try (ClusterPeerClient plain = provider.connect(grpcOnly)) {
                assertEquals("node-b", plain.nodeId());
            }

            DiscoveredNode shadowed = new DiscoveredNode("node-c", "127.0.0.1", 1, HaRole.STANDBY, Map.of(
                    ReplicationTransportProvider.AERON_CHANNEL_METADATA_KEY, "aeron:udp?endpoint=127.0.0.1:21071",
                    ReplicationTransportProvider.AERON_STREAM_ID_METADATA_KEY, "17071"));
            try (ClusterPeerClient shadow = provider.connect(shadowed)) {
                assertEquals("node-c", shadow.nodeId());
                assertThrows(IOException.class,
                        () -> shadow.fetchNodeState(TimeUnit.MILLISECONDS.toNanos(200)));
            }
        }
    }

    private static AeronReplicationTransportProvider authoritativeProvider(ServerSecurityConfig securityConfig,
                                                                           AeronPreviewTransportConfig config) {
        return new AeronReplicationTransportProvider(
                securityConfig,
                "node-a",
                "127.0.0.1",
                config,
                () -> null,
                stubSnapshotSource(),
                stubControlStateSource()
        );
    }

    private static DiscoveredNode peer(String nodeId, int port, int streamId) {
        return new DiscoveredNode(nodeId, "127.0.0.1", 1, HaRole.STANDBY, Map.of(
                ReplicationTransportProvider.AERON_CHANNEL_METADATA_KEY, "aeron:udp?endpoint=127.0.0.1:" + port,
                ReplicationTransportProvider.AERON_STREAM_ID_METADATA_KEY, Integer.toString(streamId),
                ReplicationTransportProvider.AERON_SNAPSHOT_REQUEST_CHANNEL_METADATA_KEY,
                "aeron:udp?endpoint=127.0.0.1:" + (port + 100),
                ReplicationTransportProvider.AERON_SNAPSHOT_REQUEST_STREAM_ID_METADATA_KEY,
                Integer.toString(streamId + 100),
                ReplicationTransportProvider.AERON_CONTROL_REQUEST_CHANNEL_METADATA_KEY,
                "aeron:udp?endpoint=127.0.0.1:" + (port + 300),
                ReplicationTransportProvider.AERON_CONTROL_REQUEST_STREAM_ID_METADATA_KEY,
                Integer.toString(streamId + 300)));
    }

    private static SnapshotMaterialSource stubSnapshotSource() {
        return () -> new SnapshotMaterial(Files.createTempFile("aeron-provider-snapshot", ".snap"), 0L, 0L, 0L);
    }

    private static NodeControlStateSource stubControlStateSource() {
        return () -> new NodeControlState(
                "node-a",
                HaRole.PRIMARY,
                new FencingToken(1L),
                false,
                MatchLoopState.RUNNING,
                0L,
                new ReplicationCursor(0L, 0L, 0L, 0L)
        );
    }

    private static ServerSecurityConfig secureConfig(Path dir) throws Exception {
        KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        X500Name subject = new X500Name("CN=node-a");
        Instant now = Instant.now();
        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                subject,
                BigInteger.valueOf(System.nanoTime()),
                Date.from(now.minusSeconds(60)),
                Date.from(now.plusSeconds(TimeUnit.DAYS.toSeconds(1))),
                subject,
                keyPair.getPublic()
        );
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());
        X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(builder.build(signer));

        Path certificateFile = dir.resolve("node-a.crt");
        Path privateKeyFile = dir.resolve("node-a.key");
        writePem(certificateFile, certificate);
        writePem(privateKeyFile, keyPair.getPrivate());
        return ServerSecurityConfig.fromPaths(certificateFile, privateKeyFile, certificateFile, false, 0L, false);
    }

    private static void writePem(Path path, Object entry) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
             JcaPEMWriter pem = new JcaPEMWriter(writer)) {
            pem.writeObject(entry);
        }
        assertTrue(Files.size(path) > 0L);
    }
}
