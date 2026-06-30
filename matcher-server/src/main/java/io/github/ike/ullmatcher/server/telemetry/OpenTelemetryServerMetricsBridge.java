package io.github.ike.ullmatcher.server.telemetry;

import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class OpenTelemetryServerMetricsBridge implements Closeable {
    private final List<AutoCloseable> registrations = new ArrayList<>();

    public OpenTelemetryServerMetricsBridge(String instrumentationName,
                                            Supplier<MatcherNodeMetricsSnapshot> nodeMetricsSupplier,
                                            Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier) {
        this(GlobalOpenTelemetry.getMeter(Objects.requireNonNull(instrumentationName, "instrumentationName")),
                instrumentationName,
                nodeMetricsSupplier,
                clusterMetricsSupplier);
    }

    public OpenTelemetryServerMetricsBridge(Meter meter,
                                            String prefix,
                                            Supplier<MatcherNodeMetricsSnapshot> nodeMetricsSupplier,
                                            Supplier<ClusterSupervisorMetricsSnapshot> clusterMetricsSupplier) {
        Objects.requireNonNull(meter, "meter");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(nodeMetricsSupplier, "nodeMetricsSupplier");
        Objects.requireNonNull(clusterMetricsSupplier, "clusterMetricsSupplier");
        String base = prefix.replace('-', '.').replace('_', '.');
        registrations.add(meter.gaugeBuilder(base + ".matcher.live_orders")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().liveOrderCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.last_trade_id")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().lastTradeId())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.last_applied_sequence")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().controlState().cursor().lastAppliedSequence())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.snapshot_sequence")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().latestSnapshot().lastSequence())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.loop.processed_commands")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().loopSnapshot().processedCommandCount())));
        registrations.add(meter.counterBuilder(base + ".matcher.trade_events.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().matchingMetrics().tradeCount())));
        registrations.add(meter.counterBuilder(base + ".matcher.order_events.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().matchingMetrics().orderEventCount())));
        registrations.add(meter.counterBuilder(base + ".matcher.rejected_commands.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().matchingMetrics().rejectedCommandCount())));
        registrations.add(meter.counterBuilder(base + ".matcher.capacity_rejected_commands.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().matchingMetrics().capacityRejectedCommandCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.loop.idle_polls")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().loopSnapshot().idlePollCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.loop.idle_parks")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().loopSnapshot().idleParkCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.wal.segments")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().walSegmentCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.wal.current_segment_bytes")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().currentWalSegmentBytes())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.ttl.active_tracked_orders")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().ttlMetrics().activeTrackedOrders())));
        registrations.add(meter.counterBuilder(base + ".matcher.ttl.cancel_accepted.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().ttlMetrics().cancelAcceptedTotal())));
        registrations.add(meter.counterBuilder(base + ".matcher.ttl.cancel_failed.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().ttlMetrics().cancelFailedTotal())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.submission.tracked")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().submissionMetrics().trackedCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.submission.pending")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().submissionMetrics().pendingCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.submission.committed")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().submissionMetrics().committedCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.submission.failed")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().submissionMetrics().failedCount())));
        registrations.add(meter.gaugeBuilder(base + ".matcher.submission.retrying")
                .ofLongs()
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().submissionMetrics().retryingCount())));
        registrations.add(meter.counterBuilder(base + ".matcher.submission.committed.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().submissionMetrics().committedTotal())));
        registrations.add(meter.counterBuilder(base + ".matcher.submission.failed.total")
                .buildWithCallback(m -> observe(m, nodeMetricsSupplier.get().submissionMetrics().failedTotal())));
        registrations.add(meter.counterBuilder(base + ".ha.tick.total")
                .buildWithCallback(m -> observe(m, clusterMetricsSupplier.get().tickCount())));
        registrations.add(meter.counterBuilder(base + ".ha.tick.failures.total")
                .buildWithCallback(m -> observe(m, clusterMetricsSupplier.get().tickFailureCount())));
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (AutoCloseable registration : registrations) {
            try {
                registration.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = new IOException("failed to close OpenTelemetry server metrics bridge", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        registrations.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private static void observe(ObservableLongMeasurement measurement, long value) {
        measurement.record(value);
    }
}
