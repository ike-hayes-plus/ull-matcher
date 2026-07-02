package io.github.ike.ullmatcher.ha.etcd;

import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class EtcdNodeRegistry implements NodeRegistry, Closeable {
    private static final String FIELD_SEPARATOR = "|";
    private static final String METADATA_SEPARATOR = ";";
    private static final String KV_SEPARATOR = "=";

    private final EtcdClient client;
    private final String servicePrefix;
    private final long leaseTtlSeconds;

    public EtcdNodeRegistry(EtcdConfig config) {
        this(new EtcdClient(config.endpoint(), config.timeoutMillis()),
                normalizePrefix(config.keyPrefix()) + "/discovery/",
                config.leaseTtlSeconds());
    }

    EtcdNodeRegistry(EtcdClient client, String servicePrefix, long leaseTtlSeconds) {
        this.client = Objects.requireNonNull(client, "client");
        this.servicePrefix = Objects.requireNonNull(servicePrefix, "servicePrefix");
        if (!servicePrefix.endsWith("/")) {
            throw new IllegalArgumentException("servicePrefix must end with /");
        }
        if (leaseTtlSeconds <= 0L) {
            throw new IllegalArgumentException("leaseTtlSeconds must be positive");
        }
        this.leaseTtlSeconds = leaseTtlSeconds;
    }

    @Override
    public void registerOrUpdate(DiscoveredNode node) throws IOException {
        Objects.requireNonNull(node, "node");
        long leaseId = client.grantLease(leaseTtlSeconds);
        client.put(nodeKey(node.nodeId()), encode(node), leaseId);
    }

    @Override
    public void unregister(String nodeId) throws IOException {
        client.delete(nodeKey(nodeId));
    }

    @Override
    public List<DiscoveredNode> listNodes() throws IOException {
        List<EtcdClient.KeyValue> keyValues = client.rangeByPrefix(servicePrefix);
        ArrayList<DiscoveredNode> nodes = new ArrayList<>(keyValues.size());
        for (EtcdClient.KeyValue keyValue : keyValues) {
            nodes.add(decode(keyValue.value()));
        }
        return nodes;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private String nodeKey(String nodeId) {
        return servicePrefix + nodeId;
    }

    private static String normalizePrefix(String prefix) {
        return prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
    }

    private static String encode(DiscoveredNode node) {
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
        return builder.toString();
    }

    private static DiscoveredNode decode(String payload) {
        String[] fields = payload.split("\\|", -1);
        if (fields.length != 5) {
            throw new IllegalStateException("invalid etcd discovery payload");
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
