package io.github.ike.ullmatcher.discovery.nacos;

import io.github.ike.ullmatcher.ha.discovery.DiscoveredNode;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.discovery.NodeRegistry;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class NacosNodeRegistry implements NodeRegistry {
    private static final String NODE_ID_KEY = "nodeId";
    private static final String ROLE_KEY = "role";

    private final NamingService namingService;
    private final NacosDiscoveryConfig config;

    public NacosNodeRegistry(NacosDiscoveryConfig config) throws NacosException {
        this(createNamingService(config), config);
    }

    public NacosNodeRegistry(NamingService namingService, NacosDiscoveryConfig config) {
        this.namingService = Objects.requireNonNull(namingService, "namingService");
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public void registerOrUpdate(DiscoveredNode node) throws IOException {
        Objects.requireNonNull(node, "node");
        try {
            namingService.registerInstance(config.serviceName(), config.groupName(), instanceOf(node));
        } catch (NacosException e) {
            throw new IOException("failed to register node in Nacos: " + node.nodeId(), e);
        }
    }

    @Override
    public void unregister(String nodeId) throws IOException {
        try {
            for (Instance instance : namingService.getAllInstances(config.serviceName(), config.groupName())) {
                if (nodeId.equals(instance.getMetadata().get(NODE_ID_KEY))) {
                    namingService.deregisterInstance(config.serviceName(), config.groupName(), instance.getIp(), instance.getPort(), config.clusterName());
                }
            }
        } catch (NacosException e) {
            throw new IOException("failed to unregister node from Nacos: " + nodeId, e);
        }
    }

    @Override
    public List<DiscoveredNode> listNodes() throws IOException {
        try {
            List<Instance> instances = namingService.selectInstances(config.serviceName(), config.groupName(), true);
            List<DiscoveredNode> nodes = new ArrayList<>(instances.size());
            for (Instance instance : instances) {
                nodes.add(toNode(instance));
            }
            return nodes;
        } catch (NacosException e) {
            throw new IOException("failed to list Nacos discovery nodes", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            namingService.shutDown();
        } catch (NacosException e) {
            throw new IOException("failed to close Nacos naming service", e);
        }
    }

    private Instance instanceOf(DiscoveredNode node) {
        Instance instance = new Instance();
        instance.setIp(node.host());
        instance.setPort(node.grpcPort());
        instance.setClusterName(config.clusterName());
        Map<String, String> metadata = new LinkedHashMap<>(node.metadata());
        metadata.put(NODE_ID_KEY, node.nodeId());
        metadata.put(ROLE_KEY, node.role().name());
        instance.setMetadata(metadata);
        instance.setEphemeral(true);
        instance.setHealthy(true);
        instance.setEnabled(true);
        return instance;
    }

    private static DiscoveredNode toNode(Instance instance) {
        Map<String, String> metadata = new LinkedHashMap<>(instance.getMetadata());
        String nodeId = metadata.remove(NODE_ID_KEY);
        String role = metadata.remove(ROLE_KEY);
        if (nodeId == null || role == null) {
            throw new IllegalStateException("nacos instance metadata missing nodeId or role");
        }
        return new DiscoveredNode(nodeId, instance.getIp(), instance.getPort(), HaRole.valueOf(role), metadata);
    }

    private static NamingService createNamingService(NacosDiscoveryConfig config) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", config.serverAddress());
        if (!config.namespace().isBlank()) {
            properties.setProperty("namespace", config.namespace());
        }
        return NacosFactory.createNamingService(properties);
    }
}
