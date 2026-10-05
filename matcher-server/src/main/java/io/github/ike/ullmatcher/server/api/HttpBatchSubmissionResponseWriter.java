package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.engine.SubmissionReceipt;
import io.undertow.util.Headers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes {@code POST /api/v1/orders/batch} JSON without nested {@code Map} trees.
 */
final class HttpBatchSubmissionResponseWriter {
    private static final byte[] HEADER = "{\"accepted\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MID = ",\"failed\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] COUNT = ",\"count\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUBS = ",\"submissions\":[".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX = "]}".getBytes(StandardCharsets.US_ASCII);

    private HttpBatchSubmissionResponseWriter() {
    }

    sealed interface BatchEntry permits SuccessEntry, ErrorEntry {
    }

    record SuccessEntry(int index, int status, SubmissionReceipt receipt) implements BatchEntry {
    }

    record ErrorEntry(int index, int status, String errorCode, String message) implements BatchEntry {
    }

    static void write(io.undertow.server.HttpServerExchange exchange,
                      HttpJsonCodec jsonCodec,
                      int httpStatus,
                      int accepted,
                      int failed,
                      List<BatchEntry> entries,
                      String submissionRoute,
                      String byKeyRoute) throws IOException {
        if (!jsonCodec.responseGuard(exchange).tryCommit()) {
            return;
        }
        int estimate = 256 + entries.size() * 640;
        for (BatchEntry entry : entries) {
            if (entry instanceof SuccessEntry success) {
                estimate += success.receipt().submissionId().length() + 512;
            } else if (entry instanceof ErrorEntry error) {
                estimate += error.message().length() + 128;
            }
        }
        byte[] out = new byte[estimate];
        int p = 0;
        p = copy(HEADER, out, p);
        p = writeInt(accepted, out, p);
        p = copy(MID, out, p);
        p = writeInt(failed, out, p);
        p = copy(COUNT, out, p);
        p = writeInt(entries.size(), out, p);
        p = copy(SUBS, out, p);
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) {
                out[p++] = ',';
            }
            BatchEntry entry = entries.get(i);
            if (entry instanceof SuccessEntry success) {
                p = writeSuccessEntry(out, p, success, submissionRoute, byKeyRoute);
            } else if (entry instanceof ErrorEntry error) {
                p = writeErrorEntry(out, p, error);
            }
        }
        p = copy(SUFFIX, out, p);
        if (p > out.length) {
            throw new IllegalStateException("batch json buffer overflow");
        }
        byte[] bytes = p == out.length ? out : java.util.Arrays.copyOf(out, p);
        exchange.setStatusCode(httpStatus);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, HttpJsonCodec.JSON_CONTENT_TYPE);
        exchange.getResponseSender().send(java.nio.ByteBuffer.wrap(bytes));
    }

    private static int writeSuccessEntry(byte[] out,
                                         int p,
                                         SuccessEntry entry,
                                         String submissionRoute,
                                         String byKeyRoute) {
        byte[] receiptJson = HttpSubmissionReceiptWriter.toJsonBytes(entry.receipt(), submissionRoute, byKeyRoute);
        out[p++] = '{';
        p = copy("\"index\":".getBytes(StandardCharsets.US_ASCII), out, p);
        p = writeInt(entry.index(), out, p);
        p = copy(",\"status\":".getBytes(StandardCharsets.US_ASCII), out, p);
        p = writeInt(entry.status(), out, p);
        out[p++] = ',';
        System.arraycopy(receiptJson, 1, out, p, receiptJson.length - 1);
        return p + receiptJson.length - 1;
    }

    private static int writeErrorEntry(byte[] out, int p, ErrorEntry entry) {
        out[p++] = '{';
        p = copy("\"index\":".getBytes(StandardCharsets.US_ASCII), out, p);
        p = writeInt(entry.index(), out, p);
        p = copy(",\"status\":".getBytes(StandardCharsets.US_ASCII), out, p);
        p = writeInt(entry.status(), out, p);
        p = copy(",\"code\":\"".getBytes(StandardCharsets.US_ASCII), out, p);
        p = copyAscii(entry.errorCode(), out, p);
        p = copy("\",\"error\":\"".getBytes(StandardCharsets.US_ASCII), out, p);
        p = copyAscii(escapeJson(entry.message()), out, p);
        p = copy("\"".getBytes(StandardCharsets.US_ASCII), out, p);
        out[p++] = '}';
        return p;
    }

    private static String escapeJson(String value) {
        if (value == null || value.indexOf('"') < 0 && value.indexOf('\\') < 0) {
            return value == null ? "" : value;
        }
        StringBuilder sb = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static int copy(byte[] src, byte[] dest, int destPos) {
        System.arraycopy(src, 0, dest, destPos, src.length);
        return destPos + src.length;
    }

    private static int copyAscii(String value, byte[] dest, int destPos) {
        for (int i = 0; i < value.length(); i++) {
            dest[destPos++] = (byte) value.charAt(i);
        }
        return destPos;
    }

    private static int writeInt(int value, byte[] dest, int destPos) {
        return destPos + writeLongInto(value, dest, destPos);
    }

    private static int writeLongInto(long value, byte[] dest, int destPos) {
        int start = destPos;
        if (value == 0L) {
            dest[destPos++] = '0';
            return destPos - start;
        }
        boolean negative = value < 0L;
        long current = negative ? -value : value;
        while (current > 0L) {
            dest[destPos++] = (byte) ('0' + (current % 10L));
            current /= 10L;
        }
        if (negative) {
            dest[destPos++] = '-';
        }
        int end = destPos - 1;
        int left = start;
        while (left < end) {
            byte tmp = dest[left];
            dest[left] = dest[end];
            dest[end] = tmp;
            left++;
            end--;
        }
        return destPos - start;
    }
}
