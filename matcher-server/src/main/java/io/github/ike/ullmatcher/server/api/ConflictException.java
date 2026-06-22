package io.github.ike.ullmatcher.server.api;

import org.slf4j.event.Level;

final class ConflictException extends ServerApiException {
    private static final long serialVersionUID = 1L;

    ConflictException(String message) {
        super(409, "conflict", Level.INFO, message);
    }
}
