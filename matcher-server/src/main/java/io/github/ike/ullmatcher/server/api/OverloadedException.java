package io.github.ike.ullmatcher.server.api;

import org.slf4j.event.Level;

final class OverloadedException extends ServerApiException {
    private static final long serialVersionUID = 1L;

    OverloadedException(String message) {
        super(503, "overloaded", Level.WARN, message);
    }
}
