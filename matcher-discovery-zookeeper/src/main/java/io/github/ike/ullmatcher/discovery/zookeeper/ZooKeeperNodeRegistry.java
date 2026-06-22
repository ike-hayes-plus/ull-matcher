package io.github.ike.ullmatcher.discovery.zookeeper;

import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ZooKeeperNodeRegistry implements NodeRegistry, Closeable {
    private static final String FIELD_SEPARATOR = "|";
    private static final String METADATA_SEPARATOR = ";";
    private static final String KV_SEPARATOR = "=";

    private final CuratorFramework client;
    private final String servicePath;
    private final boolean ownsClient;

    public ZooKeeperNodeRegistry(ZooKeeperDiscoveryConfig config) {
        this(
                CuratorFrameworkFactory.builder()
                        .connectString(config.connectString())
                        .sessionTimeoutMs(config.sessionTimeoutMillis())
                        .connectionTimeoutMs(config.connectionTimeoutMillis())
                        .retryPolicy(new ExponentialBackoffRetry(200, 5))
                        .build(),
                config.servicePath(),
                true
        );
    }

    private ZooKeeperNodeRegistry(CuratorFramework client, String servicePath, boolean ownsClient) {
        this.client = Objects.requireNonNull(client, "client");
        this.servicePath = Objects.requireNonNull(servicePath, "servicePath");
        this.ownsClient = ownsClient;
        if (ownsClient) {
            this.client.start();
        }
    }

    @Override
    public void registerOrUpdate(DiscoveredNode node) throws IOException {
        Objects.requireNonNull(node, "node");
        String nodePath = nodePath(node.nodeId());
        byte[] payload = encode(node);
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(nodePath, payload);
        } catch (KeeperException.NodeExistsException e) {
            try {
                Stat stat = new Stat();
                client.getData().storingStatIn(stat).forPath(nodePath);
                client.setData().withVersion(stat.getVersion()).forPath(nodePath, payload);
            } catch (Exception nested) {
                throw new IOException("failed to update ZooKeeper discovery node " + node.nodeId(), nested);
            }
        } catch (Exception e) {
            throw new IOException("failed to register ZooKeeper discovery node " + node.nodeId(), e);
        }
    }

    @Override
    public void unregister(String nodeId) throws IOException {
        try {
            client.delete().forPath(nodePath(nodeId));
        } catch (KeeperException.NoNodeException ignored) {
            return;
        } catch (Exception e) {
            throw new IOException("failed to unregister ZooKeeper discovery node " + nodeId, e);
        }
    }

    @Override
    public List<DiscoveredNode> listNodes() throws IOException {
        try {
            List<String> children = client.getChildren().forPath(servicePath);
            List<DiscoveredNode> nodes = new ArrayList<>(children.size());
            for (String child : children) {
                byte[] payload = client.getData().forPath(servicePath + "/" + child);
                nodes.add(decode(payload));
            }
            return nodes;
        } catch (KeeperException.NoNodeException ignored) {
            return List.of();
        } catch (Exception e) {
            throw new IOException("failed to list ZooKeeper discovery nodes", e);
        }
    }

    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }

    private String nodePath(String nodeId) {
        return servicePath + "/" + nodeId;
    }

    private static byte[] encode(DiscoveredNode node) {
        StringBuilder builder = new StringBuilder()
                .append(node.nodeId()).append(FIELD_SEPARATOR)
                .append(node.host()).append(FIELD_SEPARATOR)
                .append(node.grpcPort()).append(FIELD_SEPARATOR)
                .append(node.role().name()).append(FIELD_SEPARATOR);
        boolean first = true;
        for (Map.Entry<String, String> entry : node.metadata().entrySet()) {
            if (!first) {
                builder.append(METADATA_SEPARATOR);
            }
            builder.append(entry.getKey()).append(KV_SEPARATOR).append(entry.getValue());
            first = false;
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static DiscoveredNode decode(byte[] payload) {
        String[] fields = new String(payload, StandardCharsets.UTF_8).split("\\|", -1);
        if (fields.length != 5) {
            throw new IllegalStateException("invalid ZooKeeper discovery payload");
        }
        Map<String, String> metadata = new LinkedHashMap<>();
        if (!fields[4].isEmpty()) {
            for (String token : fields[4].split(METADATA_SEPARATOR)) {
                String[] kv = token.split(KV_SEPARATOR, 2);
                metadata.put(kv[0], kv.length == 2 ? kv[1] : "");
            }
        }
        return new DiscoveredNode(fields[0], fields[1], Integer.parseInt(fields[2]), HaRole.valueOf(fields[3]), metadata);
    }
}
