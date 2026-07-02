package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.ha.coordination.HaMatchRuntime;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.github.ike.ullmatcher.hft.JournaledMatcherGateway;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.storage.wal.SegmentedMmapWal;

import java.io.Closeable;
import java.io.IOException;

final class MatcherEngine implements Closeable {
    private final SegmentedMmapWal wal;
    private final SpscRingBuffer<Command> ring;
    private final UltraLowLatencyMatcher matcher;
    private final MatchLoop loop;
    private final HaMatchRuntime runtime;
    private final Thread thread;
    private final JournaledMatcherGateway gateway;
    private final StandbySyncService standbySyncService;

    MatcherEngine(SegmentedMmapWal wal,
                  SpscRingBuffer<Command> ring,
                  UltraLowLatencyMatcher matcher,
                  MatchLoop loop,
                  HaMatchRuntime runtime,
                  Thread thread,
                  JournaledMatcherGateway gateway,
                  StandbySyncService standbySyncService) {
        this.wal = wal;
        this.ring = ring;
        this.matcher = matcher;
        this.loop = loop;
        this.runtime = runtime;
        this.thread = thread;
        this.gateway = gateway;
        this.standbySyncService = standbySyncService;
    }

    SegmentedMmapWal wal() {
        return wal;
    }

    SpscRingBuffer<Command> ring() {
        return ring;
    }

    UltraLowLatencyMatcher matcher() {
        return matcher;
    }

    MatchLoop loop() {
        return loop;
    }

    HaMatchRuntime runtime() {
        return runtime;
    }

    JournaledMatcherGateway gateway() {
        return gateway;
    }

    StandbySyncService standbySyncService() {
        return standbySyncService;
    }

    @Override
    public void close() throws IOException {
        standbySyncService.close();
        loop.stop();
        try {
            thread.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while stopping matcher loop", e);
        }
        wal.close();
    }
}
