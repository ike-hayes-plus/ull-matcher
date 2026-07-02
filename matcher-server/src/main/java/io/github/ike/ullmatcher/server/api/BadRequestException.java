package io.github.ike.ullmatcher.server.api;

import org.slf4j.event.Level;

final class BadRequestException extends ServerApiException {
    private static final long serialVersionUID = 1L;

    BadRequestException(String message) {
        super(400, "bad_request", Level.DEBUG, message);
    }

    BadRequestException(String message, Throwable cause) {
        super(400, "bad_request", Level.DEBUG, message, cause);
    }
}
