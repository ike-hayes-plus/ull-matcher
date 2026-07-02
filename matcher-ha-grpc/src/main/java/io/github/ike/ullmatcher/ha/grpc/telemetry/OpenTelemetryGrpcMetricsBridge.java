package io.github.ike.ullmatcher.ha.grpc.telemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class OpenTelemetryGrpcMetricsBridge implements Closeable {
    private final List<AutoCloseable> registrations = new ArrayList<>();

    public OpenTelemetryGrpcMetricsBridge(String instrumentationName, GrpcTransportMetrics metrics) {
        this(GlobalOpenTelemetry.getMeter(Objects.requireNonNull(instrumentationName, "instrumentationName")),
                instrumentationName,
                metrics);
    }

    public OpenTelemetryGrpcMetricsBridge(Meter meter, String prefix, GrpcTransportMetrics metrics) {
        Objects.requireNonNull(meter, "meter");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(metrics, "metrics");
        String base = sanitize(prefix);
        registrations.add(meter.counterBuilder(base + ".grpc.replication.unary.total")
                .buildWithCallback(measurement -> observe(measurement, metrics.snapshot().unaryReplications())));
        registrations.add(meter.counterBuilder(base + ".grpc.replication.stream_batches.total")
                .buildWithCallback(measurement -> observe(measurement, metrics.snapshot().streamedBatches())));
        registrations.add(meter.counterBuilder(base + ".grpc.replication.stream_commands.total")
                .buildWithCallback(measurement -> observe(measurement, metrics.snapshot().streamedCommands())));
        registrations.add(meter.counterBuilder(base + ".grpc.snapshot.bytes_sent.total")
                .buildWithCallback(measurement -> observe(measurement, metrics.snapshot().snapshotBytesSent())));
        registrations.add(meter.counterBuilder(base + ".grpc.snapshot.bytes_received.total")
                .buildWithCallback(measurement -> observe(measurement, metrics.snapshot().snapshotBytesReceived())));
        registrations.add(meter.counterBuilder(base + ".grpc.ingress.rejected.total")
                .buildWithCallback(measurement -> observe(measurement, metrics.snapshot().rejectedIngress())));
        registrations.add(meter.counterBuilder(base + ".grpc.failures.total")
                .buildWithCallback(measurement -> observe(measurement, metrics.snapshot().failures())));
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (AutoCloseable registration : registrations) {
            try {
                registration.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = new IOException("failed to close OpenTelemetry gRPC metrics bridge", e);
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

    private static String sanitize(String prefix) {
        return prefix.replace('-', '.').replace('_', '.');
    }
}
