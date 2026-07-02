package io.github.ike.ullmatcher.ha.discovery;

import java.io.IOException;

/**
 * 同时提供注册和发现能力的节点注册中心抽象。
 */
public interface NodeRegistry extends DiscoveryClient, NodeRegistrar {
    @Override
    default void close() throws IOException {
        DiscoveryClient.super.close();
    }
}
