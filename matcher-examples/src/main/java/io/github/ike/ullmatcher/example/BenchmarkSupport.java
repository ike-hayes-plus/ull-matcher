package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.server.engine.MatcherNodeService;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

final class BenchmarkSupport {
    private BenchmarkSupport() {
    }

    static void quietBenchmarkLogging() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.err");
    }

    static double perSecond(long count, double seconds) {
        return seconds == 0.0 ? 0.0 : count / seconds;
    }

    static double mean(List<Double> latencies) {
        if (latencies.isEmpty()) {
            return 0.0;
        }
        double total = 0.0;
        for (double latency : latencies) {
            total += latency;
        }
        return total / latencies.size();
    }

    static double percentile(List<Double> latencies, double percentile) {
        if (latencies.isEmpty()) {
            return 0.0;
        }
        ArrayList<Double> sorted = new ArrayList<>(latencies);
        sorted.sort(Double::compareTo);
        int index = Math.max(0, Math.min(sorted.size() - 1, (int) Math.ceil(percentile * sorted.size()) - 1));
        return sorted.get(index);
    }

    static double percentileMicros(List<Long> latenciesNanos, double percentile) {
        if (latenciesNanos.isEmpty()) {
            return 0.0;
        }
        ArrayList<Long> sorted = new ArrayList<>(latenciesNanos);
        Collections.sort(sorted);
        int index = percentile >= 1.0
                ? sorted.size() - 1
                : Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(percentile * sorted.size()) - 1));
        return sorted.get(index) / 1_000.0;
    }

    static void waitForReady(MatcherNodeService service, long timeoutMillis) throws InterruptedException {
        waitFor(() -> service.health().acceptingClientCommands(), timeoutMillis, "node did not become ready");
    }

    static void waitForTrades(MatcherNodeService service, long expectedTrades, long timeoutMillis) throws InterruptedException {
        waitFor(() -> service.metricsSnapshot().matchingMetrics().tradeCount() >= expectedTrades,
                timeoutMillis,
                "timed out waiting for trade count " + expectedTrades);
    }

    static void waitForCommitted(MatcherNodeService service, long expectedCommitted, long timeoutMillis) throws InterruptedException {
        waitFor(() -> service.metricsSnapshot().submissionMetrics().committedCount() >= expectedCommitted,
                timeoutMillis,
                "timed out waiting for committed submissions " + expectedCommitted);
    }

    static void waitForStandbyDurable(MatcherNodeService standby, long sequence, long timeoutMillis) throws InterruptedException {
        waitFor(() -> standby.standbySyncService().cursor().lastDurableSequence() >= sequence,
                timeoutMillis,
                "timed out waiting for standby durable sequence " + sequence);
    }

    static void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new EOFException("channel closed while reading benchmark frame");
            }
            if (read == 0) {
                Thread.onSpinWait();
            }
        }
    }

    static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);
            if (written == 0) {
                Thread.onSpinWait();
            }
        }
    }

    private static void waitFor(BooleanSupplier condition, long timeoutMillis, String failureMessage) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new IllegalStateException(failureMessage);
    }
}
