package io.github.ike.ullmatcher.ha.grpc.telemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class OpenTelemetryGrpcMetricsBridgeTest {
    @Test
    void everyTransportCounterIsExportedUnderASanitizedPrefix() throws Exception {
        RecordingMeter meter = new RecordingMeter();
        GrpcTransportMetrics metrics = new GrpcTransportMetrics();

        try (OpenTelemetryGrpcMetricsBridge bridge =
                     new OpenTelemetryGrpcMetricsBridge(meter, "ull-matcher_node", metrics)) {
            assertNotNull(bridge);
            assertEquals(List.of(
                    "ull.matcher.node.grpc.replication.unary.total",
                    "ull.matcher.node.grpc.replication.stream_batches.total",
                    "ull.matcher.node.grpc.replication.stream_commands.total",
                    "ull.matcher.node.grpc.snapshot.bytes_sent.total",
                    "ull.matcher.node.grpc.snapshot.bytes_received.total",
                    "ull.matcher.node.grpc.ingress.rejected.total",
                    "ull.matcher.node.grpc.failures.total"
            ), List.copyOf(meter.callbacks.keySet()));
        }
    }

    @Test
    void callbacksObserveTheCurrentTransportCounters() throws Exception {
        RecordingMeter meter = new RecordingMeter();
        GrpcTransportMetrics metrics = new GrpcTransportMetrics();

        try (OpenTelemetryGrpcMetricsBridge ignored = new OpenTelemetryGrpcMetricsBridge(meter, "ull", metrics)) {
            metrics.recordUnaryReplication();
            metrics.recordStreamBatch(5);
            metrics.recordSnapshotBytesSent(1_024L);
            metrics.recordSnapshotBytesReceived(512L);
            metrics.recordRejectedIngress();
            metrics.recordRejectedIngress();
            metrics.recordFailure();

            assertEquals(1L, meter.observe("ull.grpc.replication.unary.total"));
            assertEquals(1L, meter.observe("ull.grpc.replication.stream_batches.total"));
            assertEquals(5L, meter.observe("ull.grpc.replication.stream_commands.total"));
            assertEquals(1_024L, meter.observe("ull.grpc.snapshot.bytes_sent.total"));
            assertEquals(512L, meter.observe("ull.grpc.snapshot.bytes_received.total"));
            assertEquals(2L, meter.observe("ull.grpc.ingress.rejected.total"));
            assertEquals(1L, meter.observe("ull.grpc.failures.total"));
        }
    }

    @Test
    void closeUnregistersEveryCallbackAndIsIdempotent() throws Exception {
        RecordingMeter meter = new RecordingMeter();
        OpenTelemetryGrpcMetricsBridge bridge =
                new OpenTelemetryGrpcMetricsBridge(meter, "ull", new GrpcTransportMetrics());

        bridge.close();
        assertEquals(7, meter.closed.size());

        bridge.close();
        assertEquals(7, meter.closed.size(), "a cleared registration list must not be closed twice");
    }

    @Test
    void closeFailuresAreAggregatedIntoOneIoException() {
        RecordingMeter meter = new RecordingMeter();
        meter.failOnClose = true;
        OpenTelemetryGrpcMetricsBridge bridge =
                new OpenTelemetryGrpcMetricsBridge(meter, "ull", new GrpcTransportMetrics());

        IOException error = assertThrows(IOException.class, bridge::close);

        assertEquals("failed to close OpenTelemetry gRPC metrics bridge", error.getMessage());
        assertEquals(6, error.getSuppressed().length, "the first failure carries the remaining ones as suppressed");
    }

    @Test
    void globalMeterConstructorRejectsMissingArguments() {
        GrpcTransportMetrics metrics = new GrpcTransportMetrics();

        assertThrows(NullPointerException.class, () -> new OpenTelemetryGrpcMetricsBridge(null, metrics));
        assertThrows(NullPointerException.class, () -> new OpenTelemetryGrpcMetricsBridge(null, "ull", metrics));
        assertThrows(NullPointerException.class,
                () -> new OpenTelemetryGrpcMetricsBridge(new RecordingMeter(), null, metrics));
        assertThrows(NullPointerException.class,
                () -> new OpenTelemetryGrpcMetricsBridge(new RecordingMeter(), "ull", null));
    }

    @Test
    void globalMeterConstructorRegistersAgainstTheAmbientSdk() throws Exception {
        try (OpenTelemetryGrpcMetricsBridge bridge =
                     new OpenTelemetryGrpcMetricsBridge("ull-matcher-grpc", new GrpcTransportMetrics())) {
            assertNotNull(bridge);
        }
    }

    /**
     * 只实现 bridge 真正用到的 counter 回调注册，其余 instrument 不参与本模块导出。
     */
    private static final class RecordingMeter implements Meter {
        private final Map<String, Consumer<ObservableLongMeasurement>> callbacks = new LinkedHashMap<>();
        private final List<String> closed = new ArrayList<>();
        private boolean failOnClose;

        private long observe(String name) {
            Consumer<ObservableLongMeasurement> callback = callbacks.get(name);
            assertNotNull(callback, "no callback registered for " + name);
            long[] observed = new long[1];
            callback.accept(new ObservableLongMeasurement() {
                @Override
                public void record(long value) {
                    observed[0] = value;
                }

                @Override
                public void record(long value, Attributes attributes) {
                    observed[0] = value;
                }
            });
            return observed[0];
        }

        @Override
        public LongCounterBuilder counterBuilder(String name) {
            return new RecordingCounterBuilder(name);
        }

        @Override
        public LongUpDownCounterBuilder upDownCounterBuilder(String name) {
            throw new UnsupportedOperationException("upDownCounterBuilder");
        }

        @Override
        public DoubleHistogramBuilder histogramBuilder(String name) {
            throw new UnsupportedOperationException("histogramBuilder");
        }

        @Override
        public DoubleGaugeBuilder gaugeBuilder(String name) {
            throw new UnsupportedOperationException("gaugeBuilder");
        }

        private final class RecordingCounterBuilder implements LongCounterBuilder {
            private final String name;

            private RecordingCounterBuilder(String name) {
                this.name = name;
            }

            @Override
            public LongCounterBuilder setDescription(String description) {
                return this;
            }

            @Override
            public LongCounterBuilder setUnit(String unit) {
                return this;
            }

            @Override
            public DoubleCounterBuilder ofDoubles() {
                throw new UnsupportedOperationException("ofDoubles");
            }

            @Override
            public LongCounter build() {
                throw new UnsupportedOperationException("build");
            }

            @Override
            public ObservableLongCounter buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
                callbacks.put(name, callback);
                return new ObservableLongCounter() {
                    @Override
                    public void close() {
                        if (failOnClose) {
                            throw new IllegalStateException("registration close failed for " + name);
                        }
                        closed.add(name);
                    }
                };
            }
        }
    }
}
