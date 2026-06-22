package io.github.ike.ullmatcher.server.api;

import org.slf4j.event.Level;

abstract class ServerApiException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int statusCode;
    private final String errorCode;
    private final Level logLevel;

    ServerApiException(int statusCode, String errorCode, Level logLevel, String message) {
        super(message);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.logLevel = logLevel;
    }

    ServerApiException(int statusCode, String errorCode, Level logLevel, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.logLevel = logLevel;
    }

    int statusCode() {
        return statusCode;
    }

    String errorCode() {
        return errorCode;
    }

    Level logLevel() {
        return logLevel;
    }
}
