package io.github.ike.ullmatcher.example;

import io.github.ike.ullmatcher.server.engine.MatcherNodeService;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

final class BenchmarkSupport {
    private static final int BENCHMARK_SCHEMA_VERSION = 1;

    private BenchmarkSupport() {
    }

    static void quietBenchmarkLogging() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.err");
    }

    static double perSecond(long count, double seconds) {
        return seconds == 0.0 ? 0.0 : count / seconds;
    }

    static void printJsonMetadata() {
        System.out.printf("  \"benchmarkSchemaVersion\": %d,%n", BENCHMARK_SCHEMA_VERSION);
        System.out.printf("  \"javaVersion\": \"%s\",%n", jsonString(System.getProperty("java.version", "unknown")));
        System.out.printf("  \"javaVmName\": \"%s\",%n", jsonString(System.getProperty("java.vm.name", "unknown")));
        System.out.printf("  \"osName\": \"%s\",%n", jsonString(System.getProperty("os.name", "unknown")));
        System.out.printf("  \"osArch\": \"%s\",%n", jsonString(System.getProperty("os.arch", "unknown")));
        System.out.printf("  \"availableProcessors\": %d,%n", Runtime.getRuntime().availableProcessors());
        System.out.printf("  \"gitCommit\": \"%s\",%n", jsonString(gitCommit()));
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

    static double mean(double[] latencies, int size) {
        if (size == 0) {
            return 0.0;
        }
        double total = 0.0;
        for (int i = 0; i < size; i++) {
            total += latencies[i];
        }
        return total / size;
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

    static double percentile(double[] latencies, int size, double percentile) {
        if (size == 0) {
            return 0.0;
        }
        double[] sorted = Arrays.copyOf(latencies, size);
        Arrays.sort(sorted);
        int index = Math.max(0, Math.min(size - 1, (int) Math.ceil(percentile * size) - 1));
        return sorted[index];
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

    static double percentileMicros(long[] latenciesNanos, int size, double percentile) {
        if (size == 0) {
            return 0.0;
        }
        long[] sorted = Arrays.copyOf(latenciesNanos, size);
        Arrays.sort(sorted);
        int index = percentile >= 1.0
                ? size - 1
                : Math.min(size - 1, Math.max(0, (int) Math.ceil(percentile * size) - 1));
        return sorted[index] / 1_000.0;
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

    private static String gitCommit() {
        String value = System.getenv("ULL_MATCHER_GIT_COMMIT");
        if (value == null || value.isBlank()) {
            value = System.getenv("GIT_COMMIT");
        }
        if (value != null && !value.isBlank()) {
            return value;
        }
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            if (!process.waitFor(1, TimeUnit.SECONDS) || process.exitValue() != 0) {
                process.destroyForcibly();
                return "unknown";
            }
            return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "unknown";
        } catch (IOException e) {
            return "unknown";
        }
    }

    private static String jsonString(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.toString();
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
