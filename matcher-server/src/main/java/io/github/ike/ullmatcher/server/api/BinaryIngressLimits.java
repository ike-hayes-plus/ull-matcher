package io.github.ike.ullmatcher.server.api;

/**
 * Resource limits applied to binary ingress connections.
 * <p>
 * These bound the damage an unauthenticated or stalled peer can do: each accepted connection
 * eventually allocates per-session direct buffers proportional to the configured batch size,
 * so both the connection count and the time a connection may stay idle must be capped.
 *
 * @param maxConnections maximum number of concurrently accepted connections
 * @param handshakeTimeoutMillis time a connection may stay unauthenticated before it is closed
 * @param idleTimeoutMillis time an authenticated connection may stay idle before it is closed
 */
public record BinaryIngressLimits(int maxConnections, long handshakeTimeoutMillis, long idleTimeoutMillis) {
    public static final int DEFAULT_MAX_CONNECTIONS = 4_096;
    public static final long DEFAULT_HANDSHAKE_TIMEOUT_MILLIS = 5_000L;
    public static final long DEFAULT_IDLE_TIMEOUT_MILLIS = 300_000L;

    public BinaryIngressLimits {
        if (maxConnections <= 0) {
            throw new IllegalArgumentException("maxConnections must be positive");
        }
        if (handshakeTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("handshakeTimeoutMillis must be positive");
        }
        if (idleTimeoutMillis < 0L) {
            throw new IllegalArgumentException("idleTimeoutMillis must be non-negative");
        }
    }

    public static BinaryIngressLimits defaults() {
        return new BinaryIngressLimits(
                DEFAULT_MAX_CONNECTIONS, DEFAULT_HANDSHAKE_TIMEOUT_MILLIS, DEFAULT_IDLE_TIMEOUT_MILLIS);
    }

    /**
     * Returns whether idle authenticated connections are reaped.
     *
     * @return true when an idle timeout is configured
     */
    public boolean idleTimeoutEnabled() {
        return idleTimeoutMillis > 0L;
    }
}
