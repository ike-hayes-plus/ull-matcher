package io.github.ike.ullmatcher.ha.transport;

import io.github.ike.ullmatcher.ha.replication.ReplicationTarget;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotSyncSource;
import io.github.ike.ullmatcher.ha.state.NodeControlStateClient;

import java.io.Closeable;

public interface ClusterPeerClient extends ReplicationTarget, NodeControlStateClient, SnapshotSyncSource, Closeable {
}
