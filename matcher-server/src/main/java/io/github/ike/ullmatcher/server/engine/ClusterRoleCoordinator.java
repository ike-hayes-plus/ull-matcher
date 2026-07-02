package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaCoordinator;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.coordination.HaTickResult;
import io.github.ike.ullmatcher.ha.coordination.LeaseStore;
import io.github.ike.ullmatcher.ha.failover.FailoverPolicy;
import io.github.ike.ullmatcher.ha.failover.QuorumFailoverController;
import io.github.ike.ullmatcher.ha.replication.CommandReplicator;
import io.github.ike.ullmatcher.ha.replication.ReplicationCursor;
import io.github.ike.ullmatcher.ha.replication.ReplicationMode;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.ha.state.ReplicaState;
import io.github.ike.ullmatcher.runtime.MatchLoopState;
import io.github.ike.ullmatcher.runtime.MatchLoopSnapshot;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;

import java.util.List;
import java.util.Objects;

final class ClusterRoleCoordinator {
    private final MatcherServerConfig config;
    private final QuorumFailoverController failoverController = new QuorumFailoverController();
    private final ReplicationCoordinator replicationCoordinator;

    ClusterRoleCoordinator(MatcherServerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.replicationCoordinator = new ReplicationCoordinator(config.nodeId());
    }

    void onEngineStarted(MatcherEngine engine) {
    }

    NodeControlState currentState(MatcherEngine current, boolean replicationIngressPaused, long snapshotSequence) {
        if (current == null) {
            return new NodeControlState(
                    config.nodeId(),
                    config.initialRole(),
                    new FencingToken(1L),
                    false,
                    MatchLoopState.STOPPED,
                    0L,
                    new ReplicationCursor(0L, 0L, 0L, 0L)
            );
        }
        MatchLoopSnapshot loopSnapshot = current.loop().snapshot();
        ReplicationCursor cursor = current.runtime().role() == HaRole.PRIMARY
                ? new ReplicationCursor(
                current.matcher().lastSequence(),
                current.matcher().lastSequence(),
                current.matcher().lastSequence(),
                snapshotSequence
        )
                : current.standbySyncService().cursor();
        return new NodeControlState(
                config.nodeId(),
                current.runtime().role(),
                current.runtime().fencingToken(),
                current.runtime().acceptsClientCommands() && !replicationIngressPaused,
                loopSnapshot.state(),
                loopSnapshot.processedCommandCount(),
                cursor
        );
    }

    StandbySyncService standbySyncService(MatcherEngine current, boolean replicationIngressPaused) {
        if (replicationIngressPaused) {
            throw new IllegalStateException("standby replication ingress is paused");
        }
        return current.standbySyncService();
    }

    void beginCatchUp(MatcherEngine current) {
        current.runtime().beginCatchUp();
    }

    void configureReplication(CommandReplicator replicator, ReplicationMode mode, long timeoutNanos, MatcherEngine current) {
        replicationCoordinator.configure(replicator, mode, timeoutNanos);
    }

    void clearReplication(MatcherEngine current) {
        replicationCoordinator.clear();
    }

    ReplicationCoordinator replicationCoordinator() {
        return replicationCoordinator;
    }

    HaTickResult tickHa(MatcherEngine current,
                        LeaseStore leaseStore,
                        FailoverPolicy failoverPolicy,
                        long leaseTtlNanos,
                        ReplicaState observedPrimary,
                        List<ReplicaState> standbys,
                        long nowNanos) {
        return new HaCoordinator(
                config.nodeId(),
                current.runtime(),
                leaseStore,
                failoverController,
                failoverPolicy,
                leaseTtlNanos
        ).tick(observedPrimary, standbys, nowNanos);
    }
}
