package io.github.ike.ullmatcher.ha.discovery;

import java.io.Closeable;
import java.io.IOException;

/**
 * 节点注册器。
 */
public interface NodeRegistrar extends Closeable {
    void registerOrUpdate(DiscoveredNode node) throws IOException;

    void unregister(String nodeId) throws IOException;

    @Override
    default void close() throws IOException {}
}
