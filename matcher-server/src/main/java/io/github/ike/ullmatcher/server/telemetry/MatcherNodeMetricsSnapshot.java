package io.github.ike.ullmatcher.server.telemetry;

import io.github.ike.ullmatcher.ha.standby.StandbySyncMetricsSnapshot;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.runtime.MatchLoopSnapshot;
import io.github.ike.ullmatcher.server.engine.TtlMetricsSnapshot;


public record MatcherNodeMetricsSnapshot(
        NodeControlState controlState,
        MatchLoopSnapshot loopSnapshot,
        long liveOrderCount,
        long lastTradeId,
        MatchingMetricsSnapshot matchingMetrics,
        long walSegmentCount,
        long currentWalSegmentBytes,
        SnapshotMaterial latestSnapshot,
        TtlMetricsSnapshot ttlMetrics,
        SubmitPathMetricsSnapshot submitPathMetrics,
        SubmissionMetricsSnapshot submissionMetrics,
        ReplicationCoordinatorMetricsSnapshot replicationMetrics,
        StandbySyncMetricsSnapshot standbySyncMetrics
) {
}
