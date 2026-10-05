package io.github.ike.ullmatcher.server.api;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads HTTP bodies with a hard byte cap and a single allocation when Content-Length is known.
 */
final class HttpLimitedBodies {
    private HttpLimitedBodies() {
    }

    static byte[] readAll(InputStream body, int maxBodyBytes, long declaredLength) throws IOException {
        if (maxBodyBytes <= 0) {
            throw new IllegalArgumentException("maxBodyBytes must be positive");
        }
        if (declaredLength > maxBodyBytes) {
            throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
        }
        if (declaredLength >= 0L && declaredLength <= maxBodyBytes) {
            int size = (int) declaredLength;
            byte[] exact = new byte[size];
            readFully(body, exact, 0, size, maxBodyBytes);
            return exact;
        }
        byte[] buffer = new byte[Math.min(maxBodyBytes, 4096)];
        int total = 0;
        while (true) {
            int read = body.read(buffer, total, buffer.length - total);
            if (read < 0) {
                break;
            }
            total += read;
            if (total > maxBodyBytes) {
                throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
            }
            if (total == buffer.length) {
                if (total == maxBodyBytes) {
                    throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
                }
                int nextCapacity = Math.min(maxBodyBytes, total + Math.min(total, 65536));
                byte[] grown = new byte[nextCapacity];
                System.arraycopy(buffer, 0, grown, 0, total);
                buffer = grown;
            }
        }
        if (total == buffer.length) {
            return buffer;
        }
        byte[] trimmed = new byte[total];
        System.arraycopy(buffer, 0, trimmed, 0, total);
        return trimmed;
    }

    private static void readFully(InputStream body, byte[] dest, int offset, int length, int maxBodyBytes) throws IOException {
        int readTotal = 0;
        while (readTotal < length) {
            int read = body.read(dest, offset + readTotal, length - readTotal);
            if (read < 0) {
                throw new BadRequestException("truncated request body");
            }
            readTotal += read;
            if (readTotal > maxBodyBytes) {
                throw new BadRequestException("request body exceeds max size " + maxBodyBytes + " bytes");
            }
        }
    }
}
