package io.github.ike.ullmatcher.server.api;

import org.slf4j.event.Level;

final class RequestTimeoutException extends ServerApiException {
    private static final long serialVersionUID = 1L;

    RequestTimeoutException(String message) {
        super(503, "request_timeout", Level.WARN, message);
    }
}
