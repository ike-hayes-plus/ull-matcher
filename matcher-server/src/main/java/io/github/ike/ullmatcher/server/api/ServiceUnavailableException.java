package io.github.ike.ullmatcher.server.api;

import org.slf4j.event.Level;

final class ServiceUnavailableException extends ServerApiException {
    private static final long serialVersionUID = 1L;

    ServiceUnavailableException(String message) {
        super(503, "service_unavailable", Level.WARN, message);
    }

    ServiceUnavailableException(String message, Throwable cause) {
        super(503, "service_unavailable", Level.WARN, message, cause);
    }
}
