package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class ClusterTestDoubles {
    private ClusterTestDoubles() {}

    static final class InMemoryNodeRegistry implements NodeRegistry {
        private final Map<String, DiscoveredNode> nodes = new LinkedHashMap<>();
        volatile boolean failRegister;

        @Override
        public synchronized void registerOrUpdate(DiscoveredNode node) throws IOException {
            if (failRegister) {
                throw new IOException("registry unavailable");
            }
            nodes.put(node.nodeId(), node);
        }

        @Override
        public synchronized void unregister(String nodeId) {
            nodes.remove(nodeId);
        }

        @Override
        public synchronized List<DiscoveredNode> listNodes() {
            return new ArrayList<>(nodes.values());
        }
    }

    static final class InMemoryLeaseStore implements LeaseStore {
        private volatile ClusterLease lease;

        InMemoryLeaseStore(String ownerNodeId) {
            this.lease = new ClusterLease(ownerNodeId, new FencingToken(1L), System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
        }

        @Override
        public ClusterLease currentLease() {
            return lease;
        }

        @Override
        public boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (lease == null || lease.isExpired(nowNanos) || lease.ownerNodeId().equals(nodeId)) {
                lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
                return true;
            }
            return false;
        }

        @Override
        public boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos) {
            if (lease != null && !lease.isExpired(nowNanos)
                    && lease.ownerNodeId().equals(nodeId)
                    && lease.fencingToken().equals(fencingToken)) {
                lease = new ClusterLease(nodeId, fencingToken, nowNanos + ttlNanos);
                return true;
            }
            return false;
        }
    }
}
