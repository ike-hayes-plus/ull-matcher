package io.github.ike.ullmatcher.ha.transport;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncResult;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the assembly contract a transport provider inherits: the advertised metadata key names and
 * the security snapshot default that providers without credential reloading rely on.
 */
final class ReplicationTransportProviderTest {
    @Test
    void providersWithoutCredentialReloadingInheritTheEmptySecuritySnapshot() throws IOException {
        try (FakeTransportProvider provider = new FakeTransportProvider()) {
            assertEquals(TransportSecuritySnapshot.none(), provider.securitySnapshot());
        }
    }

    @Test
    void providerExposesItsTypeAndLocalMetadataUnderTheSharedKeys() throws IOException {
        try (FakeTransportProvider provider = new FakeTransportProvider()) {
            assertEquals(ReplicationTransportType.GRPC, provider.type());
            assertEquals("GRPC",
                    provider.localNodeMetadata().get(ReplicationTransportProvider.TRANSPORT_METADATA_KEY));
            assertEquals("GRPC", provider.metricsSnapshot().transportType());
        }
    }

    @Test
    void metadataKeyNamesAreStableBecausePeersParseThem() {
        assertEquals("replicationTransport", ReplicationTransportProvider.TRANSPORT_METADATA_KEY);
        assertEquals("replicationTransportChangeWindowId",
                ReplicationTransportProvider.TRANSPORT_CHANGE_WINDOW_METADATA_KEY);
        assertEquals("aeronChannel", ReplicationTransportProvider.AERON_CHANNEL_METADATA_KEY);
        assertEquals("aeronStreamId", ReplicationTransportProvider.AERON_STREAM_ID_METADATA_KEY);
        assertEquals("aeronSnapshotRequestChannel",
                ReplicationTransportProvider.AERON_SNAPSHOT_REQUEST_CHANNEL_METADATA_KEY);
        assertEquals("aeronSnapshotRequestStreamId",
                ReplicationTransportProvider.AERON_SNAPSHOT_REQUEST_STREAM_ID_METADATA_KEY);
        assertEquals("aeronControlRequestChannel",
                ReplicationTransportProvider.AERON_CONTROL_REQUEST_CHANNEL_METADATA_KEY);
        assertEquals("aeronControlRequestStreamId",
                ReplicationTransportProvider.AERON_CONTROL_REQUEST_STREAM_ID_METADATA_KEY);
        assertEquals("aeronSecurityHandshakeRequestChannel",
                ReplicationTransportProvider.AERON_SECURITY_HANDSHAKE_REQUEST_CHANNEL_METADATA_KEY);
        assertEquals("aeronSecurityHandshakeRequestStreamId",
                ReplicationTransportProvider.AERON_SECURITY_HANDSHAKE_REQUEST_STREAM_ID_METADATA_KEY);
    }

    @Test
    void aConnectedPeerCombinesReplicationStateAndSnapshotTransfer() throws IOException {
        DiscoveredNode peer = new DiscoveredNode("node-b", "10.0.0.2", 9_090, HaRole.STANDBY, Map.of());

        try (FakeTransportProvider provider = new FakeTransportProvider()) {
            ClusterPeerClient client = provider.connect(peer);

            client.replicate(command(1L), 10L);
            client.replicateBatch(List.of(command(2L), command(3L)), 10L);
            NodeControlState state = client.fetchNodeState(10L);
            SnapshotSyncResult sync = client.downloadLatestSnapshot(Path.of("snap.bin"), 10L);
            client.close();

            assertEquals("node-b", client.nodeId());
            assertEquals(List.of(1L, 2L, 3L), ((FakePeerClient) client).replicated);
            assertEquals(HaRole.STANDBY, state.role());
            assertEquals(Path.of("snap.bin"), sync.file());
            assertTrue(((FakePeerClient) client).closed);
        }
    }

    @Test
    void closingTheProviderIsObservable() throws IOException {
        FakeTransportProvider provider = new FakeTransportProvider();

        provider.close();

        assertTrue(provider.closed);
        assertSame(ReplicationTransportType.GRPC, provider.type());
    }

    private static Command command(long sequence) {
        return Command.newOrder(sequence, 1_000L + sequence, 2_000L + sequence, 1,
                Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100L, 1L);
    }

    private static final class FakeTransportProvider implements ReplicationTransportProvider {
        private boolean closed;

        @Override
        public ReplicationTransportType type() {
            return ReplicationTransportType.GRPC;
        }

        @Override
        public Map<String, String> localNodeMetadata() {
            return Map.of(TRANSPORT_METADATA_KEY, type().name());
        }

        @Override
        public ClusterPeerClient connect(DiscoveredNode node) {
            return new FakePeerClient(node);
        }

        @Override
        public TransportMetricsSnapshot metricsSnapshot() {
            return TransportMetricsSnapshot.none(type().name());
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class FakePeerClient implements ClusterPeerClient {
        private final DiscoveredNode node;
        private final List<Long> replicated = new ArrayList<>();
        private boolean closed;

        private FakePeerClient(DiscoveredNode node) {
            this.node = node;
        }

        @Override
        public String nodeId() {
            return node.nodeId();
        }

        @Override
        public void replicate(Command command, long timeoutNanos) {
            replicated.add(command.sequence);
        }

        @Override
        public NodeControlState fetchNodeState(long timeoutNanos) {
            return new NodeControlState(node.nodeId(), node.role(), new FencingToken(1L), false,
                    MatchLoopState.QUIESCING, replicated.size(), new ReplicationCursor(0L, 0L, 0L, 0L));
        }

        @Override
        public SnapshotSyncResult downloadLatestSnapshot(Path targetFile, long timeoutNanos) {
            return new SnapshotSyncResult(targetFile, 64L, 1L, 0L, 0L);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
