package io.github.ike.ullmatcher.ha.discovery;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 节点发现客户端。
 */
public interface DiscoveryClient extends Closeable {
    List<DiscoveredNode> listNodes() throws IOException;

    @Override
    default void close() throws IOException {}
}
