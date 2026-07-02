package io.github.ike.ullmatcher.ha.transport;

public record TransportSecuritySnapshot(
        long generation,
        long reloadCount,
        long failureCount,
        boolean reloading,
        String lastError
) {
    public static TransportSecuritySnapshot none() {
        return new TransportSecuritySnapshot(0L, 0L, 0L, false, "");
    }
}
