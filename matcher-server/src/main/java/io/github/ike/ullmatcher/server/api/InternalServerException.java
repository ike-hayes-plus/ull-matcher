package io.github.ike.ullmatcher.server.api;

import org.slf4j.event.Level;

final class InternalServerException extends ServerApiException {
    private static final long serialVersionUID = 1L;

    InternalServerException(String message, Throwable cause) {
        super(500, "internal_error", Level.ERROR, message, cause);
    }
}
