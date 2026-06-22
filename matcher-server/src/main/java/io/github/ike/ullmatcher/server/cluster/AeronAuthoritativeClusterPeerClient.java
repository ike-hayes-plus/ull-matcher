package io.github.ike.ullmatcher.server.cluster;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
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
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncResult;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.server.cluster.AeronTransportSecuritySupport.AeronSecureSession;
import io.github.ike.ullmatcher.server.security.ReloadableTransportSecurityContext;
import io.github.ike.ullmatcher.server.security.TransportSecurityLoader;
import io.github.ike.ullmatcher.server.security.TransportSecurityMaterials;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.security.SecureRandom;

/**
 * 面向单个远端节点的 Aeron 权威复制客户端。
 * 负责命令复制、控制面查询、快照拉取以及安全会话建立后的消息收发。
 */
final class AeronAuthoritativeClusterPeerClient implements ClusterPeerClient, AsyncBatchReplicationTarget {
    private static final long SESSION_TTL_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final int MAX_COMMANDS_PER_BATCH_FRAME = 256;

    private final String localNodeId;
    private final String nodeId;
    private final Aeron aeron;
    private final Publication publication;
    private final String snapshotRequestChannel;
    private final int snapshotRequestStreamId;
    private final String localSnapshotResponseChannel;
    private final int localSnapshotResponseStreamId;
    private final String controlRequestChannel;
    private final int controlRequestStreamId;
    private final String localControlResponseChannel;
    private final int localControlResponseStreamId;
    private final UnsafeBuffer buffer = new UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(128 * 1024));
    private final AeronTransportMetrics metrics;
    private final ReloadableTransportSecurityContext securityContext;
    private final String securityHandshakeRequestChannel;
    private final int securityHandshakeRequestStreamId;
    private final String localSecurityHandshakeResponseChannel;
    private final int localSecurityHandshakeResponseStreamId;
    private final String localCommandAckChannel;
    private final int localCommandAckStreamId;
    private final SecureRandom secureRandom = new SecureRandom();
    private final Object publicationLock = new Object();
    private final Object controlRequestLock = new Object();
    private final Object snapshotRequestLock = new Object();
    private final Publication snapshotRequestPublication;
    private final Subscription snapshotResponseSubscription;
    private final Publication controlRequestPublication;
    private final Subscription controlResponseSubscription;
    private final Subscription commandAckSubscription;
    private final ArrayDeque<PendingAck> pendingAcks = new ArrayDeque<>();
    private final Thread ackPollerThread;
    private volatile boolean running = true;

    private volatile AeronSecureSession secureSession;
    private volatile long acknowledgedSequence;

    AeronAuthoritativeClusterPeerClient(String localNodeId,
                                        String nodeId,
                                        Aeron aeron,
                                        String channel,
                                        int streamId,
                                        String snapshotRequestChannel,
                                        int snapshotRequestStreamId,
                                        String localSnapshotResponseChannel,
                                        int localSnapshotResponseStreamId,
                                        String controlRequestChannel,
                                        int controlRequestStreamId,
                                        String localControlResponseChannel,
                                        int localControlResponseStreamId,
                                        String localCommandAckChannel,
                                        int localCommandAckStreamId,
                                        AeronTransportMetrics metrics,
                                        ReloadableTransportSecurityContext securityContext,
                                        String securityHandshakeRequestChannel,
                                        int securityHandshakeRequestStreamId,
                                        String localSecurityHandshakeResponseChannel,
                                        int localSecurityHandshakeResponseStreamId) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.aeron = Objects.requireNonNull(aeron, "aeron");
        this.publication = aeron.addExclusivePublication(Objects.requireNonNull(channel, "channel"), streamId);
        this.snapshotRequestChannel = Objects.requireNonNull(snapshotRequestChannel, "snapshotRequestChannel");
        this.snapshotRequestStreamId = snapshotRequestStreamId;
        this.localSnapshotResponseChannel = Objects.requireNonNull(localSnapshotResponseChannel, "localSnapshotResponseChannel");
        this.localSnapshotResponseStreamId = localSnapshotResponseStreamId;
        this.controlRequestChannel = Objects.requireNonNull(controlRequestChannel, "controlRequestChannel");
        this.controlRequestStreamId = controlRequestStreamId;
        this.localControlResponseChannel = Objects.requireNonNull(localControlResponseChannel, "localControlResponseChannel");
        this.localControlResponseStreamId = localControlResponseStreamId;
        this.localCommandAckChannel = Objects.requireNonNull(localCommandAckChannel, "localCommandAckChannel");
        this.localCommandAckStreamId = localCommandAckStreamId;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.securityContext = securityContext;
        this.securityHandshakeRequestChannel = securityHandshakeRequestChannel;
        this.securityHandshakeRequestStreamId = securityHandshakeRequestStreamId;
        this.localSecurityHandshakeResponseChannel = localSecurityHandshakeResponseChannel;
        this.localSecurityHandshakeResponseStreamId = localSecurityHandshakeResponseStreamId;
        this.snapshotRequestPublication = aeron.addExclusivePublication(snapshotRequestChannel, snapshotRequestStreamId);
        this.snapshotResponseSubscription = aeron.addSubscription(localSnapshotResponseChannel, localSnapshotResponseStreamId);
        this.controlRequestPublication = aeron.addExclusivePublication(controlRequestChannel, controlRequestStreamId);
        this.controlResponseSubscription = aeron.addSubscription(localControlResponseChannel, localControlResponseStreamId);
        this.commandAckSubscription = aeron.addSubscription(localCommandAckChannel, localCommandAckStreamId);
        this.ackPollerThread = Thread.ofPlatform().name("aeron-authoritative-ack-" + localNodeId + "-" + nodeId)
                .start(this::ackPollLoop);
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public void replicate(Command command, long timeoutNanos) throws IOException {
        Objects.requireNonNull(command, "command");
        replicateBatch(List.of(command), timeoutNanos);
    }

    @Override
    public void replicateBatch(List<Command> commands, long timeoutNanos) throws IOException {
        try {
            replicateBatchAsync(commands, timeoutNanos).get(timeoutNanos <= 0L ? Long.MAX_VALUE : timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            metrics.recordPublishFailure();
            throw new IOException("timed out waiting for Aeron authoritative ack", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            throw new IOException("Aeron authoritative batch replication failed", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for Aeron authoritative ack", e);
        }
    }

    @Override
    public CompletableFuture<Void> replicateBatchAsync(List<Command> commands, long timeoutNanos) {
        Objects.requireNonNull(commands, "commands");
        if (commands.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        long deadline = timeoutNanos <= 0L ? Long.MAX_VALUE : System.nanoTime() + timeoutNanos;
        long targetSequence = Long.MIN_VALUE;
        try {
            synchronized (publicationLock) {
                for (int start = 0; start < commands.size(); start += MAX_COMMANDS_PER_BATCH_FRAME) {
                    int end = Math.min(start + MAX_COMMANDS_PER_BATCH_FRAME, commands.size());
                    List<Command> frameBatch = commands.subList(start, end);
                    for (int i = start; i < end; i++) {
                        Command command = commands.get(i);
                        Objects.requireNonNull(command, "command");
                        targetSequence = Math.max(targetSequence, command.sequence);
                    }
                    offerCommand(encodeCommandBatch(frameBatch, timeoutNanos), deadline);
                }
            }
        } catch (IOException e) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
        if (targetSequence == Long.MIN_VALUE) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (pendingAcks) {
            if (acknowledgedSequence >= targetSequence) {
                future.complete(null);
                return future;
            }
            pendingAcks.addLast(new PendingAck(targetSequence, deadline, future));
        }
        return future;
    }

    @Override
    public NodeControlState fetchNodeState(long timeoutNanos) throws IOException {
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        synchronized (controlRequestLock) {
            long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
            long deadline = timeoutNanos <= 0L ? Long.MAX_VALUE : System.nanoTime() + timeoutNanos;
            AeronSecureSession session = secureSession(timeoutNanos);
            {
                metrics.recordControlRequest();
                sendControlRequest(controlRequestPublication, requestId, deadline, session);
                for (;;) {
                    NodeControlState[] stateRef = new NodeControlState[1];
                    int fragments = controlResponseSubscription.poll((controlBuffer, offset, length, header) -> {
                        AeronNodeControlStateCodec.DecodedState decoded = decodeControlState(controlBuffer, offset, length, session);
                        if (decoded != null && decoded.requestId() == requestId) {
                            stateRef[0] = decoded.state();
                        }
                    }, 8);
                    if (stateRef[0] != null) {
                        return stateRef[0];
                    }
                    if (fragments == 0) {
                        if (System.nanoTime() >= deadline) {
                            metrics.recordControlRequestFailure();
                            throw new IOException("timed out waiting for Aeron node control state from " + nodeId());
                        }
                        Thread.onSpinWait();
                    }
                }
            }
        }
    }

    @Override
    public SnapshotSyncResult downloadLatestSnapshot(Path targetFile, long timeoutNanos) throws IOException {
        Objects.requireNonNull(targetFile, "targetFile");
        if (timeoutNanos < 0L) {
            throw new IllegalArgumentException("timeoutNanos must be non-negative");
        }
        synchronized (snapshotRequestLock) {
            Path parent = targetFile.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = targetFile.resolveSibling(targetFile.getFileName() + ".part");
            long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
            long deadline = timeoutNanos <= 0L ? Long.MAX_VALUE : System.nanoTime() + timeoutNanos;
            AeronSecureSession session = secureSession(timeoutNanos);
            try (OutputStream out = Files.newOutputStream(tmp)) {
                metrics.recordSnapshotRequest();
                sendSnapshotRequest(snapshotRequestPublication, requestId, deadline, session);
                SnapshotDownloadState state = new SnapshotDownloadState();
                while (!state.completed()) {
                    int fragments = snapshotResponseSubscription.poll((snapshotBuffer, offset, length, header) -> {
                        AeronSnapshotChunkCodec.DecodedChunk decoded = decodeSnapshotChunk(snapshotBuffer, offset, length, session);
                        if (decoded == null || decoded.chunk().requestId() != requestId) {
                            return;
                        }
                        state.accept(decoded, out, metrics);
                    }, 16);
                    if (fragments == 0) {
                        if (System.nanoTime() >= deadline) {
                            metrics.recordSnapshotRequestFailure();
                            throw new IOException("timed out waiting for Aeron snapshot from " + nodeId());
                        }
                        Thread.onSpinWait();
                    }
                }
                Files.move(tmp, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                return state.toResult(targetFile);
            } catch (SnapshotDownloadException e) {
                Files.deleteIfExists(tmp);
                metrics.recordSnapshotRequestFailure();
                throw (IOException) e.getCause();
            } catch (IOException | RuntimeException e) {
                Files.deleteIfExists(tmp);
                if (!(e instanceof IOException)) {
                    metrics.recordSnapshotRequestFailure();
                    throw new IOException("Aeron snapshot download failed for " + nodeId(), e);
                }
                throw e;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        ackPollerThread.interrupt();
        try {
            ackPollerThread.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        publication.close();
        snapshotRequestPublication.close();
        snapshotResponseSubscription.close();
        controlRequestPublication.close();
        controlResponseSubscription.close();
        commandAckSubscription.close();
        aeron.close();
    }

    private void offerCommand(int length, long deadline) throws IOException {
        for (;;) {
            long result = publication.offer(buffer, 0, length);
            if (result > 0L) {
                metrics.recordPublished(length);
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                metrics.recordPublishFailure();
                throw new IOException("Aeron authoritative publication unavailable: result=" + result);
            }
            if (System.nanoTime() >= deadline) {
                metrics.recordPublishFailure();
                throw new IOException("Aeron authoritative publication timed out");
            }
            Thread.onSpinWait();
        }
    }

    private void ackPollLoop() {
        while (running) {
            boolean progressed = false;
            AeronSecureSession session = secureSession;
            int fragments = commandAckSubscription.poll((ackBuffer, offset, length, header) -> {
                long ackSequence = decodeCommandAck(ackBuffer, offset, length, session);
                if (ackSequence == Long.MIN_VALUE) {
                    return;
                }
                acknowledgedSequence = Math.max(acknowledgedSequence, ackSequence);
            }, 64);
            if (fragments > 0) {
                progressed = true;
            }
            synchronized (pendingAcks) {
                while (!pendingAcks.isEmpty()) {
                    PendingAck head = pendingAcks.peekFirst();
                    if (acknowledgedSequence >= head.targetSequence()) {
                        pendingAcks.removeFirst();
                        head.future().complete(null);
                        progressed = true;
                        continue;
                    }
                    if (System.nanoTime() >= head.deadlineNanos()) {
                        pendingAcks.removeFirst();
                        head.future().completeExceptionally(
                                new IOException("timed out waiting for Aeron authoritative ack sequence " + head.targetSequence())
                        );
                        progressed = true;
                        continue;
                    }
                    break;
                }
            }
            if (!progressed) {
                Thread.onSpinWait();
            }
        }
        synchronized (pendingAcks) {
            while (!pendingAcks.isEmpty()) {
                pendingAcks.removeFirst().future()
                        .completeExceptionally(new IOException("Aeron authoritative ack poller stopped"));
            }
        }
    }

    private record PendingAck(long targetSequence, long deadlineNanos, CompletableFuture<Void> future) {
    }

    private void sendSnapshotRequest(Publication requestPublication, long requestId, long deadline, AeronSecureSession session) throws IOException {
        UnsafeBuffer requestBuffer = AeronSnapshotRequestCodec.allocateBuffer(
                localSnapshotResponseChannel.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
        );
        int length = AeronSnapshotRequestCodec.encode(
                requestId,
                session == null ? 0L : session.sessionId(),
                localSnapshotResponseChannel,
                localSnapshotResponseStreamId,
                requestBuffer
        );
        if (session != null) {
            length = secureEnvelope(AeronTransportSecuritySupport.MessageType.SNAPSHOT_REQUEST, requestBuffer, length, session);
            requestBuffer = buffer;
        }
        for (;;) {
            long result = requestPublication.offer(requestBuffer, 0, length);
            if (result > 0L) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                metrics.recordSnapshotRequestFailure();
                throw new IOException("Aeron snapshot request publication unavailable: result=" + result);
            }
            if (System.nanoTime() >= deadline) {
                metrics.recordSnapshotRequestFailure();
                throw new IOException("Aeron snapshot request timed out");
            }
            Thread.onSpinWait();
        }
    }

    private void sendControlRequest(Publication requestPublication, long requestId, long deadline, AeronSecureSession session) throws IOException {
        UnsafeBuffer requestBuffer = AeronControlRequestCodec.allocateBuffer(
                localControlResponseChannel.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
        );
        int length = AeronControlRequestCodec.encode(
                requestId,
                session == null ? 0L : session.sessionId(),
                localControlResponseChannel,
                localControlResponseStreamId,
                requestBuffer
        );
        if (session != null) {
            length = secureEnvelope(AeronTransportSecuritySupport.MessageType.CONTROL_REQUEST, requestBuffer, length, session);
            requestBuffer = buffer;
        }
        for (;;) {
            long result = requestPublication.offer(requestBuffer, 0, length);
            if (result > 0L) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                metrics.recordControlRequestFailure();
                throw new IOException("Aeron control request publication unavailable: result=" + result);
            }
            if (System.nanoTime() >= deadline) {
                metrics.recordControlRequestFailure();
                throw new IOException("Aeron control request timed out");
            }
            Thread.onSpinWait();
        }
    }

    private int encodeCommandBatch(List<Command> commands, long timeoutNanos) throws IOException {
        UnsafeBuffer plainBuffer = AeronReplicatedCommandBatchCodec.allocateBuffer(
                localCommandAckChannel.getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
                commands.size()
        );
        int plaintextLength = AeronReplicatedCommandBatchCodec.encode(commands, localCommandAckChannel, localCommandAckStreamId, plainBuffer);
        if (securityContext == null) {
            buffer.putBytes(0, plainBuffer, 0, plaintextLength);
            return plaintextLength;
        }
        AeronSecureSession session = secureSession(timeoutNanos);
        return secureEnvelope(AeronTransportSecuritySupport.MessageType.COMMAND_BATCH, plainBuffer, plaintextLength, session);
    }

    private int secureEnvelope(int messageType, UnsafeBuffer plainBuffer, int plaintextLength, AeronSecureSession session) throws IOException {
        try {
            byte[] plaintext = new byte[plaintextLength];
            plainBuffer.getBytes(0, plaintext);
            long counter = session.nextOutgoingCounter(messageType);
            byte[] ciphertext = AeronTransportSecuritySupport.encrypt(session, messageType, counter, plaintext);
            return AeronSecureEnvelopeCodec.encode(messageType, session.sessionId(), counter, ciphertext, ciphertext.length, buffer);
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to encrypt Aeron transport message for " + nodeId, e);
        }
    }

    private AeronSnapshotChunkCodec.DecodedChunk decodeSnapshotChunk(org.agrona.DirectBuffer snapshotBuffer,
                                                                     int offset,
                                                                     int length,
                                                                     AeronSecureSession session) {
        if (session == null) {
            return AeronSnapshotChunkCodec.decode(snapshotBuffer, offset, length);
        }
        try {
            AeronSecureEnvelopeCodec.DecodedEnvelope envelope = AeronSecureEnvelopeCodec.decode(snapshotBuffer, offset, length);
            if (envelope.sessionId() != session.sessionId()) {
                return null;
            }
            byte[] plaintext = AeronTransportSecuritySupport.decrypt(session, envelope.messageType(), envelope.counter(), envelope.ciphertext());
            UnsafeBuffer plainBuffer = new UnsafeBuffer(plaintext);
            return AeronSnapshotChunkCodec.decode(plainBuffer, 0, plaintext.length);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to decrypt Aeron snapshot response from " + nodeId, e);
        }
    }

    private AeronNodeControlStateCodec.DecodedState decodeControlState(org.agrona.DirectBuffer controlBuffer,
                                                                       int offset,
                                                                       int length,
                                                                       AeronSecureSession session) {
        if (session == null) {
            return AeronNodeControlStateCodec.decode(controlBuffer, offset, length);
        }
        try {
            AeronSecureEnvelopeCodec.DecodedEnvelope envelope = AeronSecureEnvelopeCodec.decode(controlBuffer, offset, length);
            if (envelope.sessionId() != session.sessionId()) {
                return null;
            }
            byte[] plaintext = AeronTransportSecuritySupport.decrypt(session, envelope.messageType(), envelope.counter(), envelope.ciphertext());
            UnsafeBuffer plainBuffer = new UnsafeBuffer(plaintext);
            return AeronNodeControlStateCodec.decode(plainBuffer, 0, plaintext.length);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to decrypt Aeron control response from " + nodeId, e);
        }
    }

    private long decodeCommandAck(org.agrona.DirectBuffer ackBuffer, int offset, int length, AeronSecureSession session) {
        if (session == null) {
            return AeronCommandAckCodec.decode(ackBuffer, offset);
        }
        try {
            AeronSecureEnvelopeCodec.DecodedEnvelope envelope = AeronSecureEnvelopeCodec.decode(ackBuffer, offset, length);
            if (envelope.sessionId() != session.sessionId()) {
                return Long.MIN_VALUE;
            }
            if (envelope.messageType() != AeronTransportSecuritySupport.MessageType.COMMAND_ACK) {
                throw new GeneralSecurityException("unexpected secure Aeron ack message type " + envelope.messageType());
            }
            byte[] plaintext = AeronTransportSecuritySupport.decrypt(session, envelope.messageType(), envelope.counter(), envelope.ciphertext());
            return AeronCommandAckCodec.decode(new UnsafeBuffer(plaintext), 0);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to decrypt Aeron command ack from " + nodeId, e);
        }
    }

    private AeronSecureSession secureSession(long timeoutNanos) throws IOException {
        if (securityContext == null) {
            return null;
        }
        long nowMillis = System.currentTimeMillis();
        AeronSecureSession current = secureSession;
        if (current != null
                && current.generation() == securityContext.currentMaterials().generation()
                && !current.expired(nowMillis)) {
            return current;
        }
        synchronized (this) {
            current = secureSession;
            if (current != null
                    && current.generation() == securityContext.currentMaterials().generation()
                    && !current.expired(System.currentTimeMillis())) {
                return current;
            }
            secureSession = establishSecureSession(timeoutNanos);
            return secureSession;
        }
    }

    private AeronSecureSession establishSecureSession(long timeoutNanos) throws IOException {
        TransportSecurityMaterials materials = securityContext.currentMaterials();
        long nowMillis = System.currentTimeMillis();
        long expiresAtMillis = nowMillis + SESSION_TTL_MILLIS;
        long requestId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        long sessionId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        try {
            KeyPair ephemeral = AeronTransportSecuritySupport.newEphemeralKeyPair();
            byte[] clientNonce = AeronTransportSecuritySupport.randomBytes(secureRandom, 32);
            AeronSecureHandshakeRequestCodec.Request unsignedRequest = new AeronSecureHandshakeRequestCodec.Request(
                    requestId,
                    sessionId,
                    localNodeId,
                    localSecurityHandshakeResponseChannel,
                    localSecurityHandshakeResponseStreamId,
                    nowMillis,
                    expiresAtMillis,
                    clientNonce,
                    ephemeral.getPublic().getEncoded(),
                    materials.certificateChainDer(),
                    new byte[0]
            );
            byte[] signature = AeronTransportSecuritySupport.signHandshakeRequest(
                    unsignedRequest,
                    materials.privateKey(),
                    materials.leafCertificate().getPublicKey()
            );
            AeronSecureHandshakeRequestCodec.Request request = new AeronSecureHandshakeRequestCodec.Request(
                    unsignedRequest.requestId(),
                    unsignedRequest.sessionId(),
                    unsignedRequest.nodeId(),
                    unsignedRequest.responseChannel(),
                    unsignedRequest.responseStreamId(),
                    unsignedRequest.createdAtMillis(),
                    unsignedRequest.expiresAtMillis(),
                    unsignedRequest.clientNonce(),
                    unsignedRequest.clientEphemeralPublicKey(),
                    unsignedRequest.certificateChainDer(),
                    signature
            );
            UnsafeBuffer requestBuffer = AeronSecureHandshakeRequestCodec.allocateBuffer(request);
            int requestLength = AeronSecureHandshakeRequestCodec.encode(request, requestBuffer);
            long deadline = timeoutNanos <= 0L ? Long.MAX_VALUE : System.nanoTime() + timeoutNanos;
            try (Publication requestPublication = aeron.addExclusivePublication(securityHandshakeRequestChannel, securityHandshakeRequestStreamId);
                 Subscription responseSubscription = aeron.addSubscription(localSecurityHandshakeResponseChannel, localSecurityHandshakeResponseStreamId)) {
                offer(requestPublication, requestBuffer, requestLength, deadline, "Aeron secure handshake request");
                AeronSecureHandshakeResponseCodec.Response[] responseRef = new AeronSecureHandshakeResponseCodec.Response[1];
                FragmentAssembler assembler = new FragmentAssembler((responseBuffer, offset, length, header) -> {
                    AeronSecureHandshakeResponseCodec.Response response = AeronSecureHandshakeResponseCodec.decode(responseBuffer, offset, length);
                    if (response.requestId() == requestId) {
                        responseRef[0] = response;
                    }
                });
                for (;;) {
                    int fragments = responseSubscription.poll(assembler, 8);
                    if (responseRef[0] != null) {
                        AeronSecureHandshakeResponseCodec.Response response = responseRef[0];
                        if (!response.accepted()) {
                            throw new IOException("secure Aeron handshake rejected by " + nodeId + ": " + response.errorMessage());
                        }
                        var peerChain = TransportSecurityLoader.decodeCertificateChain(response.certificateChainDer());
                        TransportSecurityLoader.validatePeerCertificates(peerChain, materials.trustAnchors());
                        AeronTransportSecuritySupport.verifyHandshakeResponse(response, request, peerChain.getFirst(), System.currentTimeMillis());
                        PublicKey remotePublicKey = AeronTransportSecuritySupport.decodeX25519PublicKey(response.serverEphemeralPublicKey());
                        return AeronTransportSecuritySupport.newClientSession(
                                localNodeId,
                                nodeId,
                                materials,
                                remotePublicKey,
                                ephemeral,
                                clientNonce,
                                response.serverNonce(),
                                response.sessionId(),
                                response.expiresAtMillis()
                        );
                    }
                    if (fragments == 0) {
                        if (System.nanoTime() >= deadline) {
                            throw new IOException("timed out waiting for secure Aeron handshake response from " + nodeId);
                        }
                        Thread.onSpinWait();
                    }
                }
            }
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to establish secure Aeron session with " + nodeId, e);
        }
    }

    private void offer(Publication publication, UnsafeBuffer buffer, int length, long deadline, String operation) throws IOException {
        for (;;) {
            long result = publication.offer(buffer, 0, length);
            if (result > 0L) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IOException(operation + " unavailable: result=" + result);
            }
            if (System.nanoTime() >= deadline) {
                throw new IOException(operation + " timed out");
            }
            Thread.onSpinWait();
        }
    }

    private static final class SnapshotDownloadState {
        private long bytesWritten;
        private long lastSequence;
        private long lastTradeId;
        private long liveOrderCount;
        private int expectedChunkIndex;
        private boolean completed;

        private void accept(AeronSnapshotChunkCodec.DecodedChunk decoded, OutputStream out, AeronTransportMetrics metrics) {
            try {
                if (decoded.chunk().chunkIndex() != expectedChunkIndex) {
                    throw new IOException("snapshot chunk out of order: expected=" + expectedChunkIndex + " actual=" + decoded.chunk().chunkIndex());
                }
                byte[] payload = decoded.payload();
                out.write(payload);
                metrics.recordSnapshotBytesReceived(payload.length);
                bytesWritten += payload.length;
                lastSequence = decoded.chunk().lastSequence();
                lastTradeId = decoded.chunk().lastTradeId();
                liveOrderCount = decoded.chunk().liveOrderCount();
                expectedChunkIndex++;
                if (decoded.chunk().lastChunk()) {
                    completed = true;
                }
            } catch (IOException e) {
                throw new SnapshotDownloadException(e);
            }
        }

        private boolean completed() {
            return completed;
        }

        private SnapshotSyncResult toResult(Path targetFile) {
            return new SnapshotSyncResult(targetFile, bytesWritten, lastSequence, lastTradeId, liveOrderCount);
        }
    }

    private static final class SnapshotDownloadException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private SnapshotDownloadException(IOException cause) {
            super(cause);
        }
    }
}
