package io.github.ike.ullmatcher.server.security;

public record TlsReloadSnapshot(
        long generation,
        long reloadCount,
        long failureCount,
        boolean reloading,
        String lastError
) {
}
