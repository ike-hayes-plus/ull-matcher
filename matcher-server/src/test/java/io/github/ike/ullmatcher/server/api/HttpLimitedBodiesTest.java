package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HttpLimitedBodiesTest {
    @Test
    void readsExactContentLength() throws IOException {
        byte[] payload = "{\"ok\":true}".getBytes();
        byte[] read = HttpLimitedBodies.readAll(new ByteArrayInputStream(payload), 1024, payload.length);
        assertArrayEquals(payload, read);
    }

    @Test
    void readsChunkedBodyWithoutContentLength() throws IOException {
        byte[] payload = "abc".getBytes();
        byte[] read = HttpLimitedBodies.readAll(new ByteArrayInputStream(payload), 1024, -1L);
        assertArrayEquals(payload, read);
    }

    @Test
    void growsBufferForLargerChunkedPayload() throws IOException {
        byte[] payload = new byte[5000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xFF);
        }
        byte[] read = HttpLimitedBodies.readAll(new ByteArrayInputStream(payload), 8192, -1L);
        assertArrayEquals(payload, read);
    }

    @Test
    void rejectsDeclaredLengthAboveCap() {
        BadRequestException error = assertThrows(BadRequestException.class,
                () -> HttpLimitedBodies.readAll(InputStream.nullInputStream(), 16, 32L));
        assertEquals("request body exceeds max size 16 bytes", error.getMessage());
    }

    @Test
    void rejectsTruncatedContentLengthBody() {
        BadRequestException error = assertThrows(BadRequestException.class,
                () -> HttpLimitedBodies.readAll(new ByteArrayInputStream(new byte[]{1, 2}), 16, 4L));
        assertEquals("truncated request body", error.getMessage());
    }

    @Test
    void rejectsChunkedBodyAboveCap() {
        byte[] payload = new byte[32];
        BadRequestException error = assertThrows(BadRequestException.class,
                () -> HttpLimitedBodies.readAll(new ByteArrayInputStream(payload), 16, -1L));
        assertEquals("request body exceeds max size 16 bytes", error.getMessage());
    }

    @Test
    void rejectsInvalidMaxBodyBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> HttpLimitedBodies.readAll(InputStream.nullInputStream(), 0, -1L));
    }

    @Test
    void chunkedReadRejectsWhenGrowthWouldExceedCap() {
        byte[] payload = new byte[16];
        BadRequestException error = assertThrows(BadRequestException.class,
                () -> HttpLimitedBodies.readAll(new ByteArrayInputStream(payload), 16, -1L));
        assertEquals("request body exceeds max size 16 bytes", error.getMessage());
    }
}
