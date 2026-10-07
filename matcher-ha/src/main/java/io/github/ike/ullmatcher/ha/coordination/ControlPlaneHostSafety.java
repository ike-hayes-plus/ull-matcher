package io.github.ike.ullmatcher.ha.coordination;

import java.util.Locale;

/**
 * Shared loopback / remote-host rules for control-plane connection strings.
 */
public final class ControlPlaneHostSafety {
    private ControlPlaneHostSafety() {
    }

    public static void requireLoopbackConnectStringInProduction(String connectString, String description) {
        for (String host : hostsOf(connectString)) {
            if (!isLoopbackHost(host)) {
                throw new IllegalStateException(
                        "prod mode forbids remote " + description + " without TLS: " + host
                                + "; use a loopback ZooKeeper ensemble or etcd https");
            }
        }
    }

    public static boolean isLoopbackHost(String host) {
        if (host == null || host.isBlank()) {
            return false;
        }
        String normalized = host.trim();
        if (normalized.startsWith("[") && normalized.endsWith("]")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return "127.0.0.1".equals(normalized)
                || "localhost".equalsIgnoreCase(normalized)
                || "::1".equals(normalized);
    }

    static String[] hostsOf(String connectString) {
        if (connectString == null || connectString.isBlank()) {
            throw new IllegalArgumentException("connectString must not be blank");
        }
        String withoutChroot = connectString;
        int slash = connectString.indexOf('/');
        if (slash >= 0) {
            withoutChroot = connectString.substring(0, slash);
        }
        String[] tokens = withoutChroot.split(",");
        String[] hosts = new String[tokens.length];
        int count = 0;
        for (String token : tokens) {
            String hostPort = token.trim();
            if (hostPort.isEmpty()) {
                continue;
            }
            hosts[count++] = hostOf(hostPort);
        }
        if (count == 0) {
            throw new IllegalArgumentException("connectString must contain at least one host");
        }
        if (count != hosts.length) {
            String[] compact = new String[count];
            System.arraycopy(hosts, 0, compact, 0, count);
            return compact;
        }
        return hosts;
    }

    private static String hostOf(String hostPort) {
        if (hostPort.startsWith("[")) {
            int end = hostPort.indexOf(']');
            if (end <= 1) {
                throw new IllegalArgumentException("invalid IPv6 host: " + hostPort);
            }
            return hostPort.substring(1, end).toLowerCase(Locale.ROOT);
        }
        int colon = hostPort.lastIndexOf(':');
        if (colon <= 0) {
            return hostPort;
        }
        return hostPort.substring(0, colon);
    }
}
