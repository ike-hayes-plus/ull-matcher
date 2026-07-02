package io.github.ike.ullmatcher.server.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.ha.aeron.AeronCommandAckCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronControlRequestCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronNodeControlStateCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronReplicatedCommandBatchCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronReplicatedCommandCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronSecureEnvelopeCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeRequestCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronSecureHandshakeResponseCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronSnapshotChunkCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronSnapshotRequestCodec;
import io.github.ike.ullmatcher.ha.aeron.AeronTransportMetrics;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterialSource;
import io.github.ike.ullmatcher.ha.state.NodeControlStateSource;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.server.cluster.AeronTransportSecuritySupport.AeronSecureSession;
import io.github.ike.ullmatcher.server.security.ReloadableTransportSecurityContext;
import io.github.ike.ullmatcher.server.security.TransportSecurityLoader;
import io.github.ike.ullmatcher.server.security.TransportSecurityMaterials;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.Closeable;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

final class AeronAuthoritativeIngressService implements Closeable {
    private static final long SESSION_TTL_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final int COMMAND_POLL_LIMIT = 64;
    private static final int COMMAND_POLL_DRAIN_LIMIT = 256;
    private static final int COMMAND_ACK_FLUSH_THRESHOLD = 128;
    private static final long COMMAND_ACK_FLUSH_MAX_DELAY_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    private final String localNodeId;
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription commandSubscription;
    private final Subscription snapshotRequestSubscription;
    private final Subscription controlRequestSubscription;
    private final Subscription securityHandshakeRequestSubscription;
    private final AeronTransportMetrics metrics;
    private final Supplier<StandbySyncService> standbySyncServiceSupplier;
    private final SnapshotMaterialSource snapshotMaterialSource;
    private final NodeControlStateSource nodeControlStateSource;
    private final ReloadableTransportSecurityContext securityContext;
    private final ConcurrentHashMap<Long, AeronSecureSession> secureSessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ResponseEndpoint, ExclusivePublication> responsePublications = new ConcurrentHashMap<>();
    private final Thread pollerThread;
    private volatile boolean running = true;

    AeronAuthoritativeIngressService(String localNodeId,
                                     String channel,
                                     int streamId,
                                     String snapshotRequestChannel,
                                     int snapshotRequestStreamId,
                                     String controlRequestChannel,
                                     int controlRequestStreamId,
                                     String securityHandshakeRequestChannel,
                                     int securityHandshakeRequestStreamId,
                                     Path aeronDirectory,
                                     AeronTransportMetrics metrics,
                                     Supplier<StandbySyncService> standbySyncServiceSupplier,
                                     SnapshotMaterialSource snapshotMaterialSource,
                                     NodeControlStateSource nodeControlStateSource,
                                     ReloadableTransportSecurityContext securityContext) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(snapshotRequestChannel, "snapshotRequestChannel");
        Objects.requireNonNull(aeronDirectory, "aeronDirectory");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.standbySyncServiceSupplier = Objects.requireNonNull(standbySyncServiceSupplier, "standbySyncServiceSupplier");
        this.snapshotMaterialSource = Objects.requireNonNull(snapshotMaterialSource, "snapshotMaterialSource");
        this.nodeControlStateSource = Objects.requireNonNull(nodeControlStateSource, "nodeControlStateSource");
        this.securityContext = securityContext;
        MediaDriver.Context driverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectory.toAbsolutePath().toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        this.mediaDriver = MediaDriver.launchEmbedded(driverContext);
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
        this.commandSubscription = aeron.addSubscription(channel, streamId);
        this.snapshotRequestSubscription = aeron.addSubscription(snapshotRequestChannel, snapshotRequestStreamId);
        this.controlRequestSubscription = aeron.addSubscription(controlRequestChannel, controlRequestStreamId);
        this.securityHandshakeRequestSubscription = securityContext == null
                ? null
                : aeron.addSubscription(securityHandshakeRequestChannel, securityHandshakeRequestStreamId);
        FragmentHandler snapshotHandler = (buffer, offset, length, header) -> {
            try {
                handleSnapshotRequest(decodeSnapshotRequest(buffer, offset, length));
            } catch (IOException | RuntimeException e) {
                metrics.recordSnapshotRequestFailure();
            }
        };
        FragmentHandler controlHandler = (buffer, offset, length, header) -> {
            try {
                handleControlRequest(decodeControlRequest(buffer, offset, length));
            } catch (IOException | RuntimeException e) {
                metrics.recordControlRequestFailure();
            }
        };
        FragmentHandler handshakeHandler = (buffer, offset, length, header) -> {
            try {
                handleSecurityHandshakeRequest(AeronSecureHandshakeRequestCodec.decode(buffer, offset, length));
            } catch (IOException | RuntimeException e) {
                metrics.recordControlRequestFailure();
            }
        };
        FragmentHandler assembledHandshakeHandler = securityContext == null ? handshakeHandler : new FragmentAssembler(handshakeHandler);
        this.pollerThread = Thread.ofPlatform().name("aeron-authoritative-ingress-" + streamId)
                .start(() -> pollLoop(snapshotHandler, controlHandler, assembledHandshakeHandler));
    }

    String aeronDirectoryName() {
        return mediaDriver.aeronDirectoryName();
    }

    private void pollLoop(FragmentHandler snapshotHandler,
                          FragmentHandler controlHandler,
                          FragmentHandler handshakeHandler) {
        Map<PendingCommandAckKey, Long> pendingCommandAcks = new HashMap<>();
        int[] pendingCommandCount = new int[1];
        long[] firstPendingNanos = new long[1];
        FragmentHandler commandHandler = new FragmentAssembler((buffer, offset, length, header) -> {
            try {
                DecodedCommandReplication decoded = handleCommandReplication(buffer, offset, length, header);
                if (pendingCommandCount[0] == 0) {
                    firstPendingNanos[0] = System.nanoTime();
                }
                pendingCommandAcks.merge(
                        new PendingCommandAckKey(decoded.responseChannel(), decoded.responseStreamId(), decoded.sessionId()),
                        decoded.commands().getLast().sequence,
                        Math::max
                );
                pendingCommandCount[0] += decoded.commands().size();
                if (pendingCommandCount[0] >= COMMAND_ACK_FLUSH_THRESHOLD
                        || System.nanoTime() - firstPendingNanos[0] >= COMMAND_ACK_FLUSH_MAX_DELAY_NANOS) {
                    flushPendingCommandAcks(pendingCommandAcks, pendingCommandCount, firstPendingNanos);
                }
            } catch (IOException | RuntimeException e) {
                metrics.recordPublishFailure();
            }
        });
        while (running) {
            int fragments = 0;
            int drained;
            do {
                drained = commandSubscription.poll(commandHandler, COMMAND_POLL_LIMIT);
                fragments += drained;
            } while (drained > 0 && fragments < COMMAND_POLL_DRAIN_LIMIT);
            if (!pendingCommandAcks.isEmpty()
                    && (fragments == 0 || System.nanoTime() - firstPendingNanos[0] >= COMMAND_ACK_FLUSH_MAX_DELAY_NANOS)) {
                flushPendingCommandAcks(pendingCommandAcks, pendingCommandCount, firstPendingNanos);
            }
            fragments += snapshotRequestSubscription.poll(snapshotHandler, 4);
            fragments += controlRequestSubscription.poll(controlHandler, 8);
            if (securityHandshakeRequestSubscription != null) {
                fragments += securityHandshakeRequestSubscription.poll(handshakeHandler, 4);
            }
            if (fragments == 0) {
                Thread.onSpinWait();
            }
        }
        if (!pendingCommandAcks.isEmpty()) {
            flushPendingCommandAcks(pendingCommandAcks, pendingCommandCount, firstPendingNanos);
        }
    }

    private DecodedCommandReplication handleCommandReplication(org.agrona.DirectBuffer buffer,
                                                               int offset,
                                                               int length,
                                                               io.aeron.logbuffer.Header header) throws IOException {
        ensureIngressAllowed();
        DecodedCommandReplication decoded = decodeCommandReplication(buffer, offset, length, header);
        if (decoded.commands().size() == 1) {
            standbySyncServiceSupplier.get().appendReplicated(decoded.commands().getFirst(), 0L);
        } else {
            standbySyncServiceSupplier.get().appendReplicatedBatch(decoded.commands(), 0L);
        }
        metrics.recordReceived(length);
        return decoded;
    }

    private void flushPendingCommandAcks(Map<PendingCommandAckKey, Long> pendingCommandAcks,
                                         int[] pendingCommandCount,
                                         long[] firstPendingNanos) {
        if (pendingCommandAcks.isEmpty()) {
            return;
        }
        try {
            standbySyncServiceSupplier.get().force(pendingCommandCount[0]);
        } catch (IOException e) {
            metrics.recordPublishFailure();
            return;
        }
        pendingCommandAcks.forEach((key, sequence) -> {
            try {
                publishCommandAck(key.responseChannel(), key.responseStreamId(), sequence, key.sessionId());
            } catch (IOException e) {
                metrics.recordPublishFailure();
            }
        });
        pendingCommandAcks.clear();
        pendingCommandCount[0] = 0;
        firstPendingNanos[0] = 0L;
    }

    private void handleControlRequest(AeronControlRequestCodec.Request request) throws IOException {
        metrics.recordControlRequest();
        publishNodeControlState(
                responsePublication(request.responseChannel(), request.responseStreamId()),
                request.requestId(),
                nodeControlStateSource.currentState(),
                request.sessionId()
        );
    }

    private void handleSnapshotRequest(AeronSnapshotRequestCodec.Request request) throws IOException {
        metrics.recordSnapshotRequest();
        SnapshotMaterial snapshot = snapshotMaterialSource.latestSnapshot();
        ExclusivePublication publication = responsePublication(request.responseChannel(), request.responseStreamId());
        try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(snapshot.file()))) {
            byte[] payload = new byte[AeronSnapshotChunkCodec.MAX_CHUNK_BYTES];
            long totalBytes = Files.size(snapshot.file());
            int chunkIndex = 0;
            int read;
            while ((read = in.read(payload)) >= 0) {
                if (read == 0) {
                    continue;
                }
                publishSnapshotChunk(
                        publication,
                        new AeronSnapshotChunkCodec.Chunk(
                                request.requestId(),
                                snapshot.lastSequence(),
                                snapshot.lastTradeId(),
                                snapshot.liveOrderCount(),
                                totalBytes,
                                chunkIndex++,
                                false
                        ),
                        payload,
                        read,
                        request.sessionId()
                );
            }
            if (chunkIndex == 0) {
                publishSnapshotChunk(
                        publication,
                        new AeronSnapshotChunkCodec.Chunk(
                                request.requestId(),
                                snapshot.lastSequence(),
                                snapshot.lastTradeId(),
                                snapshot.liveOrderCount(),
                                totalBytes,
                                0,
                                true
                        ),
                        payload,
                        0,
                        request.sessionId()
                );
                return;
            }
            publishSnapshotChunk(
                    publication,
                    new AeronSnapshotChunkCodec.Chunk(
                            request.requestId(),
                            snapshot.lastSequence(),
                            snapshot.lastTradeId(),
                            snapshot.liveOrderCount(),
                            totalBytes,
                            chunkIndex,
                            true
                    ),
                    payload,
                    0,
                    request.sessionId()
            );
        }
    }

    private void publishSnapshotChunk(ExclusivePublication publication,
                                      AeronSnapshotChunkCodec.Chunk chunk,
                                      byte[] payload,
                                      int payloadLength,
                                      long sessionId) throws IOException {
        UnsafeBuffer buffer = AeronSnapshotChunkCodec.allocateBuffer();
        int length = AeronSnapshotChunkCodec.encode(chunk, payload, payloadLength, buffer);
        EncodedMessage message = maybeEncrypt(buffer, length, AeronTransportSecuritySupport.MessageType.SNAPSHOT_CHUNK, sessionId);
        offer(publication, message.buffer(), message.length());
        metrics.recordSnapshotBytesSent(payloadLength);
    }

    private void publishNodeControlState(ExclusivePublication publication, long requestId, io.github.ike.ullmatcher.ha.state.NodeControlState state)
            throws IOException {
        publishNodeControlState(publication, requestId, state, 0L);
    }

    private void publishNodeControlState(ExclusivePublication publication,
                                         long requestId,
                                         io.github.ike.ullmatcher.ha.state.NodeControlState state,
                                         long sessionId) throws IOException {
        byte[] nodeIdBytes = state.nodeId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        UnsafeBuffer buffer = AeronNodeControlStateCodec.allocateBuffer(nodeIdBytes.length);
        int length = AeronNodeControlStateCodec.encode(requestId, state, buffer);
        EncodedMessage message = maybeEncrypt(buffer, length, AeronTransportSecuritySupport.MessageType.NODE_CONTROL_STATE, sessionId);
        offer(publication, message.buffer(), message.length());
    }

    private void offer(ExclusivePublication publication, UnsafeBuffer buffer, int length) throws IOException {
        for (;;) {
            long result = publication.offer(buffer, 0, length);
            if (result > 0L) {
                return;
            }
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IOException("Aeron snapshot publication unavailable: result=" + result);
            }
            if (!running) {
                throw new IOException("Aeron snapshot publication stopped while sending snapshot");
            }
            Thread.onSpinWait();
        }
    }

    private void ensureIngressAllowed() {
        HaRole role = nodeControlStateSource.currentState().role();
        if (role != HaRole.STANDBY && role != HaRole.CATCHING_UP) {
            throw new IllegalStateException("node is not accepting Aeron standby replication ingress");
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        try {
            pollerThread.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while closing Aeron authoritative ingress", e);
        } finally {
            CloseHelper.quietClose(commandSubscription);
            CloseHelper.quietClose(snapshotRequestSubscription);
            CloseHelper.quietClose(controlRequestSubscription);
            CloseHelper.quietClose(securityHandshakeRequestSubscription);
            responsePublications.values().forEach(CloseHelper::quietClose);
            CloseHelper.quietClose(aeron);
            CloseHelper.quietClose(mediaDriver);
        }
    }

    private DecodedCommandReplication decodeCommandReplication(org.agrona.DirectBuffer buffer,
                                                               int offset,
                                                               int length,
                                                               io.aeron.logbuffer.Header header) {
        if (securityContext == null) {
            int frameKind = AeronReplicatedCommandCodec.frameKind(buffer, offset);
            if (frameKind == AeronReplicatedCommandCodec.FRAME_KIND_SINGLE) {
                AeronReplicatedCommandCodec.DecodedCommand decoded = AeronReplicatedCommandCodec.decode(buffer, offset, length);
                return DecodedCommandReplication.single(decoded.command(), decoded.responseChannel(), decoded.responseStreamId(), 0L);
            }
            if (frameKind == AeronReplicatedCommandBatchCodec.FRAME_KIND_BATCH) {
                AeronReplicatedCommandBatchCodec.DecodedBatch decoded = AeronReplicatedCommandBatchCodec.decode(buffer, offset, length);
                return new DecodedCommandReplication(decoded.commands(), decoded.responseChannel(), decoded.responseStreamId(), 0L);
            }
            throw new IllegalArgumentException("unsupported Aeron replicated frame kind " + frameKind);
        }
        try {
            AeronSecureEnvelopeCodec.DecodedEnvelope envelope = AeronSecureEnvelopeCodec.decode(buffer, offset, length);
            if (envelope.messageType() != AeronTransportSecuritySupport.MessageType.COMMAND
                    && envelope.messageType() != AeronTransportSecuritySupport.MessageType.COMMAND_BATCH) {
                throw new GeneralSecurityException("unexpected secure Aeron message type " + envelope.messageType());
            }
            AeronSecureSession session = activeSession(envelope.sessionId());
            byte[] plaintext = AeronTransportSecuritySupport.decrypt(session, envelope.messageType(), envelope.counter(), envelope.ciphertext());
            UnsafeBuffer plainBuffer = new UnsafeBuffer(plaintext);
            int frameKind = AeronReplicatedCommandCodec.frameKind(plainBuffer, 0);
            if (frameKind == AeronReplicatedCommandCodec.FRAME_KIND_SINGLE) {
                AeronReplicatedCommandCodec.DecodedCommand decoded = AeronReplicatedCommandCodec.decode(plainBuffer, 0, plaintext.length);
                return DecodedCommandReplication.single(decoded.command(), decoded.responseChannel(), decoded.responseStreamId(), envelope.sessionId());
            }
            if (frameKind == AeronReplicatedCommandBatchCodec.FRAME_KIND_BATCH) {
                AeronReplicatedCommandBatchCodec.DecodedBatch decoded = AeronReplicatedCommandBatchCodec.decode(plainBuffer, 0, plaintext.length);
                return new DecodedCommandReplication(decoded.commands(), decoded.responseChannel(), decoded.responseStreamId(), envelope.sessionId());
            }
            throw new GeneralSecurityException("unsupported secure Aeron replicated frame kind " + frameKind);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to decrypt Aeron command", e);
        }
    }

    private AeronSnapshotRequestCodec.Request decodeSnapshotRequest(org.agrona.DirectBuffer buffer, int offset, int length) {
        if (securityContext == null) {
            return AeronSnapshotRequestCodec.decode(buffer, offset, length);
        }
        try {
            byte[] plaintext = decryptSecureEnvelope(buffer, offset, length, AeronTransportSecuritySupport.MessageType.SNAPSHOT_REQUEST);
            return AeronSnapshotRequestCodec.decode(new UnsafeBuffer(plaintext), 0, plaintext.length);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to decrypt Aeron snapshot request", e);
        }
    }

    private AeronControlRequestCodec.Request decodeControlRequest(org.agrona.DirectBuffer buffer, int offset, int length) {
        if (securityContext == null) {
            return AeronControlRequestCodec.decode(buffer, offset, length);
        }
        try {
            byte[] plaintext = decryptSecureEnvelope(buffer, offset, length, AeronTransportSecuritySupport.MessageType.CONTROL_REQUEST);
            return AeronControlRequestCodec.decode(new UnsafeBuffer(plaintext), 0, plaintext.length);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to decrypt Aeron control request", e);
        }
    }

    private byte[] decryptSecureEnvelope(org.agrona.DirectBuffer buffer, int offset, int length, int expectedMessageType)
            throws GeneralSecurityException {
        AeronSecureEnvelopeCodec.DecodedEnvelope envelope = AeronSecureEnvelopeCodec.decode(buffer, offset, length);
        if (envelope.messageType() != expectedMessageType) {
            throw new GeneralSecurityException("unexpected secure Aeron message type " + envelope.messageType());
        }
        AeronSecureSession session = activeSession(envelope.sessionId());
        return AeronTransportSecuritySupport.decrypt(session, envelope.messageType(), envelope.counter(), envelope.ciphertext());
    }

    private EncodedMessage maybeEncrypt(UnsafeBuffer plainBuffer, int plaintextLength, int messageType, long sessionId) throws IOException {
        if (securityContext == null) {
            return new EncodedMessage(plainBuffer, plaintextLength);
        }
        if (sessionId == 0L) {
            throw new IOException("secure Aeron response requires an established session");
        }
        try {
            AeronSecureSession session = activeSession(sessionId);
            byte[] plaintext = new byte[plaintextLength];
            plainBuffer.getBytes(0, plaintext);
            long counter = session.nextOutgoingCounter(messageType);
            byte[] ciphertext = AeronTransportSecuritySupport.encrypt(session, messageType, counter, plaintext);
            UnsafeBuffer secured = AeronSecureEnvelopeCodec.allocateBuffer(ciphertext.length);
            int length = AeronSecureEnvelopeCodec.encode(messageType, session.sessionId(), counter, ciphertext, ciphertext.length, secured);
            return new EncodedMessage(secured, length);
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to encrypt Aeron response message", e);
        }
    }

    private void publishCommandAck(String responseChannel, int responseStreamId, long sequence, long sessionId) throws IOException {
        UnsafeBuffer buffer = AeronCommandAckCodec.allocateBuffer();
        int length = AeronCommandAckCodec.encode(sequence, buffer);
        EncodedMessage message = maybeEncrypt(buffer, length, AeronTransportSecuritySupport.MessageType.COMMAND_ACK, sessionId);
        offer(responsePublication(responseChannel, responseStreamId), message.buffer(), message.length());
    }

    private ExclusivePublication responsePublication(String channel, int streamId) {
        ResponseEndpoint endpoint = new ResponseEndpoint(channel, streamId);
        return responsePublications.computeIfAbsent(endpoint, key -> aeron.addExclusivePublication(key.channel(), key.streamId()));
    }

    private AeronSecureSession activeSession(long sessionId) throws GeneralSecurityException {
        AeronSecureSession session = secureSessions.get(sessionId);
        if (session == null) {
            throw new GeneralSecurityException("unknown secure Aeron session " + sessionId);
        }
        if (session.generation() != securityContext.currentMaterials().generation() || session.expired(System.currentTimeMillis())) {
            secureSessions.remove(sessionId);
            throw new GeneralSecurityException("expired secure Aeron session " + sessionId);
        }
        return session;
    }

    private void handleSecurityHandshakeRequest(AeronSecureHandshakeRequestCodec.Request request) throws IOException {
        long nowMillis = System.currentTimeMillis();
        AeronSecureHandshakeResponseCodec.Response response;
        try {
            TransportSecurityMaterials materials = securityContext.currentMaterials();
            var peerChain = TransportSecurityLoader.decodeCertificateChain(request.certificateChainDer());
            TransportSecurityLoader.validatePeerCertificates(peerChain, materials.trustAnchors());
            AeronTransportSecuritySupport.verifyHandshakeRequest(request, peerChain.getFirst(), nowMillis);
            KeyPair localEphemeral = AeronTransportSecuritySupport.newEphemeralKeyPair();
            PublicKey remoteEphemeral = AeronTransportSecuritySupport.decodeX25519PublicKey(request.clientEphemeralPublicKey());
            byte[] serverNonce = AeronTransportSecuritySupport.randomBytes(new java.security.SecureRandom(), 32);
            long expiresAtMillis = Math.min(nowMillis + SESSION_TTL_MILLIS, request.expiresAtMillis());
            AeronSecureHandshakeResponseCodec.Response unsigned = new AeronSecureHandshakeResponseCodec.Response(
                    request.requestId(),
                    request.sessionId(),
                    true,
                    localNodeId,
                    nowMillis,
                    expiresAtMillis,
                    "",
                    serverNonce,
                    localEphemeral.getPublic().getEncoded(),
                    materials.certificateChainDer(),
                    new byte[0]
            );
            byte[] signature = AeronTransportSecuritySupport.signHandshakeResponse(
                    unsigned,
                    request,
                    materials.privateKey(),
                    materials.leafCertificate().getPublicKey()
            );
            response = new AeronSecureHandshakeResponseCodec.Response(
                    unsigned.requestId(),
                    unsigned.sessionId(),
                    true,
                    unsigned.nodeId(),
                    unsigned.createdAtMillis(),
                    unsigned.expiresAtMillis(),
                    unsigned.errorMessage(),
                    unsigned.serverNonce(),
                    unsigned.serverEphemeralPublicKey(),
                    unsigned.certificateChainDer(),
                    signature
            );
            secureSessions.put(request.sessionId(), AeronTransportSecuritySupport.newServerSession(
                    localNodeId,
                    request.nodeId(),
                    materials,
                    localEphemeral,
                    remoteEphemeral,
                    request.clientNonce(),
                    serverNonce,
                    request.sessionId(),
                    expiresAtMillis
            ));
        } catch (GeneralSecurityException e) {
            response = new AeronSecureHandshakeResponseCodec.Response(
                    request.requestId(),
                    request.sessionId(),
                    false,
                    "",
                    nowMillis,
                    nowMillis,
                    e.getMessage(),
                    new byte[0],
                    new byte[0],
                    java.util.List.of(),
                    new byte[0]
            );
        }
        try (ExclusivePublication publication = aeron.addExclusivePublication(request.responseChannel(), request.responseStreamId())) {
            UnsafeBuffer buffer = AeronSecureHandshakeResponseCodec.allocateBuffer(response);
            int length = AeronSecureHandshakeResponseCodec.encode(response, buffer);
            offer(publication, buffer, length);
        }
    }

    private record EncodedMessage(UnsafeBuffer buffer, int length) {
    }

    private record ResponseEndpoint(String channel, int streamId) {
    }

    private record PendingCommandAckKey(String responseChannel, int responseStreamId, long sessionId) {
    }

    private record DecodedCommandReplication(java.util.List<Command> commands,
                                             String responseChannel,
                                             int responseStreamId,
                                             long sessionId) {
        private static DecodedCommandReplication single(Command command, String responseChannel, int responseStreamId, long sessionId) {
            return new DecodedCommandReplication(java.util.List.of(command), responseChannel, responseStreamId, sessionId);
        }
    }
}
