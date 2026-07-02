package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import io.github.ike.ullmatcher.ha.coordination.HaMatchRuntime;
import io.github.ike.ullmatcher.ha.coordination.HaRole;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterial;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.hft.JournaledMatcherGateway;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;
import io.github.ike.ullmatcher.storage.replay.ReplayService;
import io.github.ike.ullmatcher.storage.wal.SegmentedMmapWal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;

final class EngineLifecycleManager {
    private static final Logger LOG = LoggerFactory.getLogger(EngineLifecycleManager.class);
    private final MatcherServerConfig config;
    private final TtlCancelGuard ttlCancelGuard;
    private final OrderStateTracker orderStateTracker;
    private final SnapshotCoordinator snapshotCoordinator;
    private final ClusterRoleCoordinator clusterRoleCoordinator;

    EngineLifecycleManager(MatcherServerConfig config,
                           TtlCancelGuard ttlCancelGuard,
                           OrderStateTracker orderStateTracker,
                           SnapshotCoordinator snapshotCoordinator,
                           ClusterRoleCoordinator clusterRoleCoordinator) {
        this.config = config;
        this.ttlCancelGuard = ttlCancelGuard;
        this.orderStateTracker = orderStateTracker;
        this.snapshotCoordinator = snapshotCoordinator;
        this.clusterRoleCoordinator = clusterRoleCoordinator;
    }

    EngineStartResult createEngine(HaRole role, FencingToken token) throws IOException {
        SegmentedMmapWal wal = new SegmentedMmapWal(config.walDirectory(), config.walPrefix(), config.walSegmentSizeBytes());
        MatchEventHandler eventHandler = new CompositeMatchEventHandler(noopHandler(), ttlCancelGuard, orderStateTracker);
        ttlCancelGuard.bindEventSink(eventHandler);
        SnapshotCoordinator.RestoredSnapshot restored = snapshotCoordinator.restore(eventHandler);
        long snapshotSequence = restored.snapshotSequence();
        var matcher = restored.matcher();
        wal.resetReader();
        ReplayService.replay(wal, matcher, snapshotSequence);
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(config.ringCapacity());
        MatchLoop loop = new MatchLoop(ring, matcher, config.loopConfig(),
                (command, error) -> LOG.error("matcher loop failed nodeId={} sequence={} type={}",
                        config.nodeId(), command.sequence, command.type, error));
        HaMatchRuntime runtime = new HaMatchRuntime(config.nodeId(), loop, role, token);
        Thread thread = Thread.ofPlatform().name("matcher-" + config.nodeId()).start(loop);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                wal,
                ring,
                config.gatewaySpinLimit(),
                config.gatewayOfferTimeoutNanos(),
                runtime::acceptsClientCommands,
                config.walDurabilityMode(),
                config.walForceBatchSize(),
                config.walForceMaxDelayMicros()
        );
        StandbySyncService standbySyncService = new StandbySyncService(config.nodeId(), wal, ring, matcher, config.standbySyncConfig());
        if (restored.snapshotMaterial() != null) {
            standbySyncService.markSnapshot(snapshotSequence);
        }
        MatcherEngine engine = new MatcherEngine(wal, ring, matcher, loop, runtime, thread, gateway, standbySyncService);
        clusterRoleCoordinator.onEngineStarted(engine);
        return new EngineStartResult(engine, restored.snapshotMaterial());
    }

    RestartResult restartFromSnapshot(MatcherEngine current) throws IOException {
        HaRole role = current.runtime().role();
        FencingToken token = current.runtime().fencingToken();
        current.close();
        EngineStartResult restarted = createEngine(role, token);
        return new RestartResult(restarted.engine(), restarted.snapshotMaterial(), role);
    }

    private static MatchEventHandler noopHandler() {
        return new MatchEventHandler() {
            @Override
            public void onTrade(io.github.ike.ullmatcher.api.TradeEvent event) {
            }

            @Override
            public void onOrder(io.github.ike.ullmatcher.api.OrderEvent event) {
            }
        };
    }

    record EngineStartResult(MatcherEngine engine, SnapshotMaterial snapshotMaterial) {
    }

    record RestartResult(MatcherEngine engine, SnapshotMaterial snapshotMaterial, HaRole role) {
    }
}
