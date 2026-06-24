package io.github.ike.ullmatcher.server.cluster;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.api.TradeEvent;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.aeron.AeronTransportMetrics;
import io.github.ike.ullmatcher.ha.aeron.AeronCommandAckCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronReplicatedCommandCodec;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.standby.StandbySyncConfig;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import io.github.ike.ullmatcher.server.security.ReloadableTransportSecurityContext;
import io.github.ike.ullmatcher.server.security.ServerSecurityConfig;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
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
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AeronAuthoritativeClusterPeerClientTest {
    @Test
    void authoritativeClientPublishesAndWaitsForStandbyAck() throws Exception {
        String channel = "aeron:udp?endpoint=127.0.0.1:16021";
        int streamId = 12021;
        String snapshotRequestChannel = "aeron:udp?endpoint=127.0.0.1:16121";
        int snapshotRequestStreamId = 12121;
        String snapshotResponseChannel = "aeron:udp?endpoint=127.0.0.1:16221";
        int snapshotResponseStreamId = 12221;
        String controlRequestChannel = "aeron:udp?endpoint=127.0.0.1:16321";
        int controlRequestStreamId = 12321;
        String controlResponseChannel = "aeron:udp?endpoint=127.0.0.1:16421";
        int controlResponseStreamId = 12421;
        String commandAckChannel = "aeron:udp?endpoint=127.0.0.1:16721";
        int commandAckStreamId = 12721;
        String handshakeRequestChannel = "aeron:udp?endpoint=127.0.0.1:16521";
        int handshakeRequestStreamId = 12521;
        String handshakeResponseChannel = "aeron:udp?endpoint=127.0.0.1:16621";
        int handshakeResponseStreamId = 12621;
        Path dir = Files.createTempDirectory("aeron-authoritative");
        try (StandbyFixture standby = new StandbyFixture(
                channel,
                streamId,
                snapshotRequestChannel,
                snapshotRequestStreamId,
                controlRequestChannel,
                controlRequestStreamId,
                handshakeRequestChannel,
                handshakeRequestStreamId,
                dir)) {
            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(standby.aeronDirectoryName()));
                 AeronAuthoritativeClusterPeerClient client = new AeronAuthoritativeClusterPeerClient(
                         "primary-a",
                         "standby-a",
                         aeron,
                         channel,
                         streamId,
                         snapshotRequestChannel,
                         snapshotRequestStreamId,
                         snapshotResponseChannel,
                         snapshotResponseStreamId,
                         controlRequestChannel,
                         controlRequestStreamId,
                         controlResponseChannel,
                         controlResponseStreamId,
                         commandAckChannel,
                         commandAckStreamId,
                         new AeronTransportMetrics(),
                         null,
                         handshakeRequestChannel,
                         handshakeRequestStreamId,
                         handshakeResponseChannel,
                         handshakeResponseStreamId)) {
                client.replicate(command(1L), TimeUnit.SECONDS.toNanos(2));
                NodeControlState state = client.fetchNodeState(TimeUnit.SECONDS.toNanos(1));
                assertEquals(1L, state.cursor().lastReceivedSequence());
                assertEquals(1, standby.wal.appendCount);
                Path downloaded = Files.createTempFile("snapshot-copy", ".snap");
                var result = client.downloadLatestSnapshot(downloaded, TimeUnit.SECONDS.toNanos(2));
                assertEquals(Files.readString(standby.snapshotFile), Files.readString(downloaded));
                assertEquals(Files.size(standby.snapshotFile), result.bytesWritten());
            }
        }
    }

    @Test
    void authoritativeClientSupportsMutualTlsHandshakeAndAeadPayloads() throws Exception {
        SecureFixture secure = SecureFixture.create(Files.createTempDirectory("aeron-secure-fixture"));
        String channel = "aeron:udp?endpoint=127.0.0.1:17021";
        int streamId = 13021;
        String snapshotRequestChannel = "aeron:udp?endpoint=127.0.0.1:17121";
        int snapshotRequestStreamId = 13121;
        String snapshotResponseChannel = "aeron:udp?endpoint=127.0.0.1:17221";
        int snapshotResponseStreamId = 13221;
        String controlRequestChannel = "aeron:udp?endpoint=127.0.0.1:17321";
        int controlRequestStreamId = 13321;
        String controlResponseChannel = "aeron:udp?endpoint=127.0.0.1:17421";
        int controlResponseStreamId = 13421;
        String commandAckChannel = "aeron:udp?endpoint=127.0.0.1:17721";
        int commandAckStreamId = 13721;
        String handshakeRequestChannel = "aeron:udp?endpoint=127.0.0.1:17521";
        int handshakeRequestStreamId = 13521;
        String handshakeResponseChannel = "aeron:udp?endpoint=127.0.0.1:17621";
        int handshakeResponseStreamId = 13621;
        Path dir = Files.createTempDirectory("aeron-authoritative-secure");
        var clientMaterials = secure.clientContext().currentMaterials();
        var serverMaterials = secure.serverContext().currentMaterials();
        var probeRequest = new io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeRequestCodec.Request(
                1L,
                2L,
                "primary-a",
                handshakeResponseChannel,
                handshakeResponseStreamId,
                System.currentTimeMillis(),
                System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5),
                new byte[32],
                AeronTransportSecuritySupport.newEphemeralKeyPair().getPublic().getEncoded(),
                clientMaterials.certificateChainDer(),
                new byte[0]
        );
        byte[] probeSignature = AeronTransportSecuritySupport.signHandshakeRequest(
                probeRequest,
                clientMaterials.privateKey(),
                clientMaterials.leafCertificate().getPublicKey()
        );
        var signedProbeRequest = new io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeRequestCodec.Request(
                probeRequest.requestId(),
                probeRequest.sessionId(),
                probeRequest.nodeId(),
                probeRequest.responseChannel(),
                probeRequest.responseStreamId(),
                probeRequest.createdAtMillis(),
                probeRequest.expiresAtMillis(),
                probeRequest.clientNonce(),
                probeRequest.clientEphemeralPublicKey(),
                probeRequest.certificateChainDer(),
                probeSignature
        );
        var probeBuffer = io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeRequestCodec.allocateBuffer(signedProbeRequest);
        int probeLength = io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeRequestCodec.encode(signedProbeRequest, probeBuffer);
        var decodedProbeRequest = io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeRequestCodec.decode(probeBuffer, 0, probeLength);
        var peerChain = io.github.ike.ullmatcher.server.security.TransportSecurityLoader.decodeCertificateChain(
                decodedProbeRequest.certificateChainDer()
        );
        io.github.ike.ullmatcher.server.security.TransportSecurityLoader.validatePeerCertificates(peerChain, serverMaterials.trustAnchors());
        AeronTransportSecuritySupport.verifyHandshakeRequest(decodedProbeRequest, peerChain.getFirst(), System.currentTimeMillis());
        try (secure;
             StandbyFixture standby = new StandbyFixture(
                     channel,
                     streamId,
                     snapshotRequestChannel,
                     snapshotRequestStreamId,
                     controlRequestChannel,
                     controlRequestStreamId,
                     handshakeRequestChannel,
                     handshakeRequestStreamId,
                     dir,
                     secure.serverContext())) {
            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(standby.aeronDirectoryName()));
                 AeronAuthoritativeClusterPeerClient client = new AeronAuthoritativeClusterPeerClient(
                         "primary-a",
                         "standby-a",
                         aeron,
                         channel,
                         streamId,
                         snapshotRequestChannel,
                         snapshotRequestStreamId,
                         snapshotResponseChannel,
                         snapshotResponseStreamId,
                         controlRequestChannel,
                         controlRequestStreamId,
                         controlResponseChannel,
                         controlResponseStreamId,
                         commandAckChannel,
                         commandAckStreamId,
                         new AeronTransportMetrics(),
                         secure.clientContext(),
                         handshakeRequestChannel,
                         handshakeRequestStreamId,
                         handshakeResponseChannel,
                         handshakeResponseStreamId)) {
                client.replicate(command(2L), TimeUnit.SECONDS.toNanos(2));
                NodeControlState state = client.fetchNodeState(TimeUnit.SECONDS.toNanos(1));
                assertEquals(2L, state.cursor().lastReceivedSequence());
                Path downloaded = Files.createTempFile("secure-snapshot-copy", ".snap");
                var result = client.downloadLatestSnapshot(downloaded, TimeUnit.SECONDS.toNanos(2));
                assertEquals(Files.readString(standby.snapshotFile), Files.readString(downloaded));
                assertEquals(Files.size(standby.snapshotFile), result.bytesWritten());
            }
        }
    }

    @Test
    void ingressBatchesCommandAcksPerResponseEndpoint() throws Exception {
        String channel = "aeron:udp?endpoint=127.0.0.1:18021";
        int streamId = 14021;
        String snapshotRequestChannel = "aeron:udp?endpoint=127.0.0.1:18121";
        int snapshotRequestStreamId = 14121;
        String controlRequestChannel = "aeron:udp?endpoint=127.0.0.1:18321";
        int controlRequestStreamId = 14321;
        String handshakeRequestChannel = "aeron:udp?endpoint=127.0.0.1:18521";
        int handshakeRequestStreamId = 14521;
        String ackChannel = "aeron:udp?endpoint=127.0.0.1:18721";
        int ackStreamId = 14721;
        Path dir = Files.createTempDirectory("aeron-batched-ack");
        try (StandbyFixture standby = new StandbyFixture(
                channel,
                streamId,
                snapshotRequestChannel,
                snapshotRequestStreamId,
                controlRequestChannel,
                controlRequestStreamId,
                handshakeRequestChannel,
                handshakeRequestStreamId,
                dir);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(standby.aeronDirectoryName()));
             Publication publication = aeron.addExclusivePublication(channel, streamId);
             Subscription ackSubscription = aeron.addSubscription(ackChannel, ackStreamId)) {
            for (long sequence = 1L; sequence <= 3L; sequence++) {
                UnsafeBuffer commandBuffer = AeronReplicatedCommandCodec.allocateBuffer(ackChannel.getBytes(StandardCharsets.UTF_8).length);
                int length = AeronReplicatedCommandCodec.encode(command(sequence), ackChannel, ackStreamId, commandBuffer);
                offer(publication, commandBuffer, length, TimeUnit.SECONDS.toNanos(2));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            long highestAck = Long.MIN_VALUE;
            while (System.nanoTime() < deadline && highestAck < 3L) {
                final long[] ackRef = {highestAck};
                int fragments = ackSubscription.poll((buffer, offset, length, header) -> {
                    ackRef[0] = Math.max(ackRef[0], AeronCommandAckCodec.decode(buffer, offset));
                }, 8);
                highestAck = ackRef[0];
                if (fragments == 0) {
                    Thread.onSpinWait();
                }
            }
            assertEquals(3L, highestAck);
            assertEquals(3, standby.wal.appendCount);
        }
    }

    @Test
    void authoritativeClientReplicatesBatchAndWaitsForLastAck() throws Exception {
        String channel = "aeron:udp?endpoint=127.0.0.1:19021";
        int streamId = 15021;
        String snapshotRequestChannel = "aeron:udp?endpoint=127.0.0.1:19121";
        int snapshotRequestStreamId = 15121;
        String snapshotResponseChannel = "aeron:udp?endpoint=127.0.0.1:19221";
        int snapshotResponseStreamId = 15221;
        String controlRequestChannel = "aeron:udp?endpoint=127.0.0.1:19321";
        int controlRequestStreamId = 15321;
        String controlResponseChannel = "aeron:udp?endpoint=127.0.0.1:19421";
        int controlResponseStreamId = 15421;
        String commandAckChannel = "aeron:udp?endpoint=127.0.0.1:19721";
        int commandAckStreamId = 15721;
        String handshakeRequestChannel = "aeron:udp?endpoint=127.0.0.1:19521";
        int handshakeRequestStreamId = 15521;
        String handshakeResponseChannel = "aeron:udp?endpoint=127.0.0.1:19621";
        int handshakeResponseStreamId = 15621;
        Path dir = Files.createTempDirectory("aeron-authoritative-batch");
        try (StandbyFixture standby = new StandbyFixture(
                channel,
                streamId,
                snapshotRequestChannel,
                snapshotRequestStreamId,
                controlRequestChannel,
                controlRequestStreamId,
                handshakeRequestChannel,
                handshakeRequestStreamId,
                dir)) {
            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(standby.aeronDirectoryName()));
                 AeronAuthoritativeClusterPeerClient client = new AeronAuthoritativeClusterPeerClient(
                         "primary-a",
                         "standby-a",
                         aeron,
                         channel,
                         streamId,
                         snapshotRequestChannel,
                         snapshotRequestStreamId,
                         snapshotResponseChannel,
                         snapshotResponseStreamId,
                         controlRequestChannel,
                         controlRequestStreamId,
                         controlResponseChannel,
                         controlResponseStreamId,
                         commandAckChannel,
                         commandAckStreamId,
                         new AeronTransportMetrics(),
                         null,
                         handshakeRequestChannel,
                         handshakeRequestStreamId,
                         handshakeResponseChannel,
                         handshakeResponseStreamId)) {
                client.replicateBatch(List.of(command(20L), command(21L), command(22L)), TimeUnit.SECONDS.toNanos(2));
                NodeControlState state = client.fetchNodeState(TimeUnit.SECONDS.toNanos(1));
                assertEquals(22L, state.cursor().lastReceivedSequence());
                assertEquals(3, standby.wal.appendCount);
            }
        }
    }

    @Test
    void transportSecurityContextReloadsRotatedCertificates() throws Exception {
        Path dir = Files.createTempDirectory("transport-security-reload");
        CertificateAuthority authority = CertificateAuthority.create("CN=ULL Reload CA");
        IdentityMaterial initial = authority.issue("CN=node-a", dir.resolve("node-a"));
        Path caPath = dir.resolve("ca.crt");
        writePem(caPath, authority.certificate());
        try (ReloadableTransportSecurityContext context = new ReloadableTransportSecurityContext(
                ServerSecurityConfig.fromPaths(
                        initial.certificatePath(),
                        initial.privateKeyPath(),
                        caPath,
                        true,
                        100L,
                        false
                ))) {
            String originalFingerprint = context.currentMaterials().certificateFingerprint();
            IdentityMaterial rotated = authority.issue("CN=node-a-rotated", dir.resolve("node-a-rotated"));
            Files.copy(rotated.certificatePath(), initial.certificatePath(), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(rotated.privateKeyPath(), initial.privateKeyPath(), StandardCopyOption.REPLACE_EXISTING);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline && context.snapshot().reloadCount() == 0L) {
                Thread.sleep(100L);
            }
            assertTrue(context.snapshot().reloadCount() > 0L);
            assertTrue(context.snapshot().generation() > 1L);
            assertTrue(!originalFingerprint.equals(context.currentMaterials().certificateFingerprint()));
        }
    }

    @Test
    void closeStopsAckPollerThread() throws Exception {
        String channel = "aeron:udp?endpoint=127.0.0.1:20021";
        int streamId = 16021;
        String snapshotRequestChannel = "aeron:udp?endpoint=127.0.0.1:20121";
        int snapshotRequestStreamId = 16121;
        String snapshotResponseChannel = "aeron:udp?endpoint=127.0.0.1:20221";
        int snapshotResponseStreamId = 16221;
        String controlRequestChannel = "aeron:udp?endpoint=127.0.0.1:20321";
        int controlRequestStreamId = 16321;
        String controlResponseChannel = "aeron:udp?endpoint=127.0.0.1:20421";
        int controlResponseStreamId = 16421;
        String commandAckChannel = "aeron:udp?endpoint=127.0.0.1:20721";
        int commandAckStreamId = 16721;
        String handshakeRequestChannel = "aeron:udp?endpoint=127.0.0.1:20521";
        int handshakeRequestStreamId = 16521;
        String handshakeResponseChannel = "aeron:udp?endpoint=127.0.0.1:20621";
        int handshakeResponseStreamId = 16621;
        Path dir = Files.createTempDirectory("aeron-authoritative-close");
        Thread ackPoller;
        try (StandbyFixture standby = new StandbyFixture(
                channel,
                streamId,
                snapshotRequestChannel,
                snapshotRequestStreamId,
                controlRequestChannel,
                controlRequestStreamId,
                handshakeRequestChannel,
                handshakeRequestStreamId,
                dir);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(standby.aeronDirectoryName()))) {
            AeronAuthoritativeClusterPeerClient client = new AeronAuthoritativeClusterPeerClient(
                    "primary-a",
                    "standby-a",
                    aeron,
                    channel,
                    streamId,
                    snapshotRequestChannel,
                    snapshotRequestStreamId,
                    snapshotResponseChannel,
                    snapshotResponseStreamId,
                    controlRequestChannel,
                    controlRequestStreamId,
                    controlResponseChannel,
                    controlResponseStreamId,
                    commandAckChannel,
                    commandAckStreamId,
                    new AeronTransportMetrics(),
                    null,
                    handshakeRequestChannel,
                    handshakeRequestStreamId,
                    handshakeResponseChannel,
                    handshakeResponseStreamId
            );
            client.replicate(command(30L), TimeUnit.SECONDS.toNanos(2));
            ackPoller = ackPollerThread(client);
            client.close();
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (ackPoller.isAlive() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(!ackPoller.isAlive());
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 10_000L + sequence, 1L, 1, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
    }

    private static Thread ackPollerThread(AeronAuthoritativeClusterPeerClient client) throws ReflectiveOperationException {
        Field field = AeronAuthoritativeClusterPeerClient.class.getDeclaredField("ackPollerThread");
        field.setAccessible(true);
        return (Thread) field.get(client);
    }

    private static final class StandbyFixture implements AutoCloseable {
        private final RecordingWalWriter wal = new RecordingWalWriter();
        private final SpscRingBuffer<Command> ring = new SpscRingBuffer<>(16);
        private final UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(MatcherConfig.defaults(1), new NoopHandler());
        private final MatchLoop loop = new MatchLoop(ring, matcher);
        private final Thread loopThread = Thread.ofPlatform().start(loop);
        private final StandbySyncService standby =
                new StandbySyncService("standby-a", wal, ring, matcher, StandbySyncConfig.defaults());
        private final Path snapshotFile;
        private final AeronAuthoritativeIngressService ingressService;

        private StandbyFixture(String channel,
                               int streamId,
                               String snapshotRequestChannel,
                               int snapshotRequestStreamId,
                               String controlRequestChannel,
                               int controlRequestStreamId,
                               String handshakeRequestChannel,
                               int handshakeRequestStreamId,
                               Path dir) throws IOException {
            this(channel, streamId, snapshotRequestChannel, snapshotRequestStreamId, controlRequestChannel, controlRequestStreamId,
                    handshakeRequestChannel, handshakeRequestStreamId, dir, null);
        }

        private StandbyFixture(String channel,
                               int streamId,
                               String snapshotRequestChannel,
                               int snapshotRequestStreamId,
                               String controlRequestChannel,
                               int controlRequestStreamId,
                               String handshakeRequestChannel,
                               int handshakeRequestStreamId,
                               Path dir,
                               ReloadableTransportSecurityContext securityContext) throws IOException {
            this.snapshotFile = Files.createTempFile("snapshot", ".snap");
            Files.writeString(snapshotFile, "snapshot-payload");
            this.ingressService = new AeronAuthoritativeIngressService(
                    "standby-a",
                    channel,
                    streamId,
                    snapshotRequestChannel,
                    snapshotRequestStreamId,
                    controlRequestChannel,
                    controlRequestStreamId,
                    handshakeRequestChannel,
                    handshakeRequestStreamId,
                    dir,
                    new AeronTransportMetrics(),
                    () -> standby,
                    () -> new io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial(snapshotFile, 7L, 3L, 1L),
                    () -> new NodeControlState(
                            "standby-a",
                            HaRole.STANDBY,
                            new FencingToken(1L),
                            false,
                            MatchLoopState.RUNNING,
                            standby.cursor().lastAppliedSequence(),
                            standby.cursor()
                    ),
                    securityContext
            );
        }

        private String aeronDirectoryName() {
            return ingressService.aeronDirectoryName();
        }

        @Override
        public void close() throws IOException {
            IOException error = null;
            try {
                ingressService.close();
            } catch (IOException e) {
                error = e;
            } finally {
                try {
                    standby.close();
                } catch (IOException e) {
                    if (error == null) {
                        error = e;
                    } else {
                        error.addSuppressed(e);
                    }
                }
                loop.stop();
            }
            Files.deleteIfExists(snapshotFile);
            try {
                loopThread.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping standby fixture", e);
            }
            if (error != null) {
                throw error;
            }
        }
    }

    private static final class RecordingWalWriter implements WalWriter {
        private int appendCount;

        @Override
        public void append(Command command) {
            appendCount++;
        }

        @Override
        public void force() {}

        @Override
        public void close() {}
    }

    private static final class NoopHandler implements MatchEventHandler {
        @Override
        public void onTrade(TradeEvent event) {}

        @Override
        public void onOrder(OrderEvent event) {}
    }

    private record SecureFixture(ReloadableTransportSecurityContext clientContext,
                                 ReloadableTransportSecurityContext serverContext) implements AutoCloseable {
        private static SecureFixture create(Path dir) throws Exception {
            Files.createDirectories(dir);
            CertificateAuthority authority = CertificateAuthority.create("CN=ULL Test CA");
            IdentityMaterial clientIdentity = authority.issue("CN=primary-a", dir.resolve("client"));
            IdentityMaterial serverIdentity = authority.issue("CN=standby-a", dir.resolve("server"));
            Path caPath = dir.resolve("ca.crt");
            writePem(caPath, authority.certificate());
            return new SecureFixture(
                    new ReloadableTransportSecurityContext(ServerSecurityConfig.fromPaths(
                            clientIdentity.certificatePath(),
                            clientIdentity.privateKeyPath(),
                            caPath,
                            true,
                            100L,
                            false
                    )),
                    new ReloadableTransportSecurityContext(ServerSecurityConfig.fromPaths(
                            serverIdentity.certificatePath(),
                            serverIdentity.privateKeyPath(),
                            caPath,
                            true,
                            100L,
                            false
                    ))
            );
        }

        @Override
        public void close() throws IOException {
            IOException error = null;
            try {
                clientContext.close();
            } catch (IOException e) {
                error = e;
            }
            try {
                serverContext.close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
            if (error != null) {
                throw error;
            }
        }
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
            X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(builder.build(signer));
            return new CertificateAuthority(certificate, keyPair);
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

    private static void writePemChain(Path path, Object... entries) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
             JcaPEMWriter pem = new JcaPEMWriter(writer)) {
            for (Object entry : entries) {
                pem.writeObject(entry);
            }
        }
    }

    private static void writePem(Path path, Object entry) throws IOException {
        writePemChain(path, entry);
        assertTrue(Files.size(path) > 0L);
    }

    private static void offer(Publication publication, UnsafeBuffer buffer, int length, long timeoutNanos) throws IOException {
        long deadline = System.nanoTime() + timeoutNanos;
        for (;;) {
            long result = publication.offer(buffer, 0, length);
            if (result > 0L) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IOException("test publication unavailable: result=" + result);
            }
            if (System.nanoTime() >= deadline) {
                throw new IOException("test publication timed out");
            }
            Thread.onSpinWait();
        }
    }
}
