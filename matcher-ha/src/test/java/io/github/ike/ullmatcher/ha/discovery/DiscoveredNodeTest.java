package io.github.ike.ullmatcher.ha.discovery;

import io.github.ike.ullmatcher.ha.coordination.HaRole;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Discovery records are used to dial peers, so a malformed entry must be rejected at construction
 * instead of producing an unusable endpoint string later.
 */
final class DiscoveredNodeTest {
    @Test
    void endpointJoinsHostAndPort() {
        DiscoveredNode node = new DiscoveredNode("node-a", "10.0.0.7", 9_090, HaRole.PRIMARY, Map.of());

        assertEquals("10.0.0.7:9090", node.endpoint());
        assertEquals(HaRole.PRIMARY, node.role());
        assertEquals(Map.of(), node.metadata());
    }

    @Test
    void nullMetadataBecomesAnEmptyMap() {
        DiscoveredNode node = new DiscoveredNode("node-a", "host", 1, HaRole.STANDBY, null);

        assertEquals(Map.of(), node.metadata());
    }

    @Test
    void metadataIsDefensivelyCopiedAndImmutable() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("replicationTransport", "GRPC");
        DiscoveredNode node = new DiscoveredNode("node-a", "host", 1, HaRole.STANDBY, metadata);

        metadata.put("replicationTransport", "AERON");
        metadata.put("extra", "value");

        assertEquals(Map.of("replicationTransport", "GRPC"), node.metadata());
        assertThrows(UnsupportedOperationException.class, () -> node.metadata().put("k", "v"));
    }

    @Test
    void portBoundariesAreAcceptedAndOutOfRangePortsAreRejected() {
        assertEquals("host:1", new DiscoveredNode("n", "host", 1, HaRole.STANDBY, Map.of()).endpoint());
        assertEquals("host:65535", new DiscoveredNode("n", "host", 65_535, HaRole.STANDBY, Map.of()).endpoint());

        assertThrows(IllegalArgumentException.class,
                () -> new DiscoveredNode("n", "host", 0, HaRole.STANDBY, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DiscoveredNode("n", "host", -1, HaRole.STANDBY, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DiscoveredNode("n", "host", 65_536, HaRole.STANDBY, Map.of()));
    }

    @Test
    void blankIdentityIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new DiscoveredNode("", "host", 1, HaRole.STANDBY, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DiscoveredNode("   ", "host", 1, HaRole.STANDBY, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DiscoveredNode("n", "", 1, HaRole.STANDBY, Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DiscoveredNode("n", "\t", 1, HaRole.STANDBY, Map.of()));
    }

    @Test
    void missingIdentityIsRejected() {
        assertThrows(NullPointerException.class,
                () -> new DiscoveredNode(null, "host", 1, HaRole.STANDBY, Map.of()));
        assertThrows(NullPointerException.class,
                () -> new DiscoveredNode("n", null, 1, HaRole.STANDBY, Map.of()));
        assertThrows(NullPointerException.class,
                () -> new DiscoveredNode("n", "host", 1, null, Map.of()));
    }

    @Test
    void nodesWithTheSameIdentityAndMetadataAreEqual() {
        DiscoveredNode first = new DiscoveredNode("node-a", "host", 1, HaRole.STANDBY, Map.of("k", "v"));
        DiscoveredNode second = new DiscoveredNode("node-a", "host", 1, HaRole.STANDBY, Map.of("k", "v"));

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertTrue(first.toString().contains("node-a"));
    }
}
