package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestBodyLimitInputStreamTest {
    @Test
    void allowsReadsUpToLimit() throws IOException {
        byte[] buffer = new byte[4];
        try (RequestBodyLimitInputStream input = new RequestBodyLimitInputStream(
                new ByteArrayInputStream(new byte[] {1, 2, 3, 4}), 4)) {
            assertEquals(4, input.read(buffer, 0, buffer.length));
            assertEquals(-1, input.read());
        }
    }

    @Test
    void rejectsReadsBeyondLimit() throws IOException {
        byte[] buffer = new byte[4];
        try (RequestBodyLimitInputStream input = new RequestBodyLimitInputStream(
                new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5}), 4)) {
            assertEquals(4, input.read(buffer, 0, buffer.length));
            assertThrows(BadRequestException.class, input::read);
        }
    }
}
