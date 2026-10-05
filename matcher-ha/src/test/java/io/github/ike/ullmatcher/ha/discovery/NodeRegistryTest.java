package io.github.ike.ullmatcher.ha.discovery;

import io.github.ike.ullmatcher.ha.coordination.HaRole;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A registry inherits {@code close()} from two super-interfaces; the explicit override exists so the
 * default resolves unambiguously to the discovery side rather than failing to compile. These tests
 * pin that resolution and the register/unregister round trip of an in-memory implementation.
 */
final class NodeRegistryTest {
    @Test
    void registerThenListThenUnregisterRoundTrips() throws IOException {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        DiscoveredNode primary = node("node-a", HaRole.PRIMARY);
        DiscoveredNode standby = node("node-b", HaRole.STANDBY);

        registry.registerOrUpdate(primary);
        registry.registerOrUpdate(standby);

        assertEquals(List.of(primary, standby), registry.listNodes());

        registry.unregister("node-a");

        assertEquals(List.of(standby), registry.listNodes());
    }

    @Test
    void registeringTheSameNodeIdReplacesTheExistingEntry() throws IOException {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        registry.registerOrUpdate(node("node-a", HaRole.STANDBY));

        registry.registerOrUpdate(node("node-a", HaRole.PRIMARY));

        assertEquals(1, registry.listNodes().size());
        assertEquals(HaRole.PRIMARY, registry.listNodes().getFirst().role());
    }

    @Test
    void unregisteringAnUnknownNodeIsANoOp() throws IOException {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();

        registry.unregister("never-registered");

        assertTrue(registry.listNodes().isEmpty());
    }

    @Test
    void registryCloseResolvesToTheDiscoveryClientDefaultAndReleasesNothing() throws IOException {
        InMemoryNodeRegistry registry = new InMemoryNodeRegistry();
        registry.registerOrUpdate(node("node-a", HaRole.PRIMARY));

        registry.close();
        registry.close();

        assertEquals(1, registry.listNodes().size(),
                "the inherited default close must be a repeatable no-op");
    }

    @Test
    void discoveryClientAndRegistrarDefaultCloseAreNoOps() throws IOException {
        List<String> events = new ArrayList<>();
        DiscoveryClient discoveryOnly = () -> List.of();
        NodeRegistrar registrarOnly = new NodeRegistrar() {
            @Override
            public void registerOrUpdate(DiscoveredNode node) {
                events.add("register");
            }

            @Override
            public void unregister(String nodeId) {
                events.add("unregister");
            }
        };

        discoveryOnly.close();
        registrarOnly.close();

        assertTrue(events.isEmpty());
        assertTrue(discoveryOnly.listNodes().isEmpty());
    }

    private static DiscoveredNode node(String nodeId, HaRole role) {
        return new DiscoveredNode(nodeId, "10.0.0.1", 9_090, role, Map.of());
    }

    private static final class InMemoryNodeRegistry implements NodeRegistry {
        private final Map<String, DiscoveredNode> nodes = new LinkedHashMap<>();

        @Override
        public List<DiscoveredNode> listNodes() {
            return List.copyOf(nodes.values());
        }

        @Override
        public void registerOrUpdate(DiscoveredNode node) {
            nodes.put(node.nodeId(), node);
        }

        @Override
        public void unregister(String nodeId) {
            nodes.remove(nodeId);
        }
    }
}
