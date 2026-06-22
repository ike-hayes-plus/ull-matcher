package io.github.ike.ullmatcher.server.bootstrap;

public final class ServerBootstrapException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ServerBootstrapException(String message) {
        super(message);
    }

    public ServerBootstrapException(String message, Throwable cause) {
        super(message, cause);
    }
}
