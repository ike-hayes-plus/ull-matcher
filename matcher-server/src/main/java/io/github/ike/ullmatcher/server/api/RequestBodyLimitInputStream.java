package io.github.ike.ullmatcher.server.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

final class RequestBodyLimitInputStream extends InputStream {
    private final InputStream delegate;
    private final int maxBodyBytes;
    private int totalRead;

    RequestBodyLimitInputStream(InputStream delegate, int maxBodyBytes) {
        if (maxBodyBytes <= 0) {
            throw new IllegalArgumentException("maxBodyBytes must be positive");
        }
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.maxBodyBytes = maxBodyBytes;
    }

    @Override
    public int read() throws IOException {
        int value = delegate.read();
        if (value >= 0) {
            increment(1);
        }
        return value;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = delegate.read(b, off, len);
        if (read > 0) {
            increment(read);
        }
        return read;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private void increment(int read) throws IOException {
        totalRead += read;
        if (totalRead > maxBodyBytes) {
            throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
        }
    }
}
