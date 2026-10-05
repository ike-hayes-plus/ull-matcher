package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.engine.SubmissionReceipt;
import io.undertow.util.Headers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Writes submission receipt JSON without building intermediate {@code Map} objects.
 */
final class HttpSubmissionReceiptWriter {
    private static final byte[] PREFIX = "{\"submissionId\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MID = "\",\"idempotencyKey\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] OP = ",\"operationType\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] USER = "\",\"userId\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ORDER = ",\"orderId\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SEQ = ",\"sequence\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PHASE = ",\"phase\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESULT = "\",\"localResult\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RESULT_ALIAS = "\",\"result\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] LOCAL_DUR = "\",\"localDurable\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] REP_REQ = ",\"replicationRequired\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] REP_COM = ",\"replicationCommitted\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TGT = ",\"totalTargets\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] REQ_ACK = ",\"requiredAcks\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ACKED = ",\"ackedTargets\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RETRY = ",\"retryCount\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] LAST_ERR = ",\"lastError\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CREATED = ",\"createdAtEpochMillis\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] UPDATED = ",\"updatedAtEpochMillis\":".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] QUERY = ",\"queryPath\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] QUERY_KEY = "\",\"queryByIdempotencyPath\":\"".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SUFFIX = "\"}".getBytes(StandardCharsets.US_ASCII);

    private HttpSubmissionReceiptWriter() {
    }

    static byte[] toJsonBytes(SubmissionReceipt receipt, String submissionRoute, String byKeyRoute) {
        String localResult = receipt.localResult() == null ? null : receipt.localResult().name();
        boolean nullResult = receipt.localResult() == null;
        String idempotencyKey = receipt.idempotencyKey() == null ? "" : receipt.idempotencyKey();
        int estimate = 640
                + receipt.submissionId().length()
                + idempotencyKey.length()
                + receipt.operationType().length()
                + receipt.phase().name().length()
                + (nullResult ? 4 : localResult.length() * 2)
                + submissionRoute.length()
                + byKeyRoute.length()
                + idempotencyKey.length();
        byte[] out = new byte[estimate];
        int p = 0;
        p = copy(PREFIX, out, p);
        p = copyAscii(receipt.submissionId(), out, p);
        p = copy(MID, out, p);
        if (receipt.idempotencyKey() == null) {
            p = copy("null".getBytes(StandardCharsets.US_ASCII), out, p);
        } else {
            p = copy("\"".getBytes(StandardCharsets.US_ASCII), out, p);
            p = copyAscii(idempotencyKey, out, p);
            p = copy("\"".getBytes(StandardCharsets.US_ASCII), out, p);
        }
        p = copy(OP, out, p);
        p = copyAscii(receipt.operationType(), out, p);
        p = copy(USER, out, p);
        p = writeLong(receipt.userId(), out, p);
        p = copy(ORDER, out, p);
        p = writeLong(receipt.orderId(), out, p);
        p = copy(SEQ, out, p);
        p = writeLong(receipt.sequence(), out, p);
        p = copy(PHASE, out, p);
        p = copyAscii(receipt.phase().name(), out, p);
        if (nullResult) {
            p = copy("\",\"localResult\":null,\"result\":null".getBytes(StandardCharsets.US_ASCII), out, p);
        } else {
            p = copy(RESULT, out, p);
            p = copyAscii(localResult, out, p);
            p = copy(RESULT_ALIAS, out, p);
            p = copyAscii(localResult, out, p);
        }
        p = copy(LOCAL_DUR, out, p);
        p = writeBool(receipt.localDurable(), out, p);
        p = copy(REP_REQ, out, p);
        p = writeBool(receipt.replicationRequired(), out, p);
        p = copy(REP_COM, out, p);
        p = writeBool(receipt.replicationCommitted(), out, p);
        p = copy(TGT, out, p);
        p = writeInt(receipt.totalTargets(), out, p);
        p = copy(REQ_ACK, out, p);
        p = writeInt(receipt.requiredAcks(), out, p);
        p = copy(ACKED, out, p);
        p = writeInt(receipt.ackedTargets(), out, p);
        p = copy(RETRY, out, p);
        p = writeLong(receipt.retryCount(), out, p);
        p = copy(LAST_ERR, out, p);
        if (receipt.lastError() == null) {
            p = copy("null".getBytes(StandardCharsets.US_ASCII), out, p);
        } else {
            p = copy("\"".getBytes(StandardCharsets.US_ASCII), out, p);
            p = copyAscii(receipt.lastError(), out, p);
            p = copy("\"".getBytes(StandardCharsets.US_ASCII), out, p);
        }
        p = copy(CREATED, out, p);
        p = writeLong(receipt.createdAtEpochMillis(), out, p);
        p = copy(UPDATED, out, p);
        p = writeLong(receipt.updatedAtEpochMillis(), out, p);
        p = copy(QUERY, out, p);
        p = copyAscii(submissionRoute.replace("{submissionId}", receipt.submissionId()), out, p);
        p = copy(QUERY_KEY, out, p);
        p = copyAscii(byKeyRoute, out, p);
        p = copy("?idempotencyKey=".getBytes(StandardCharsets.US_ASCII), out, p);
        p = appendUrlEncoded(idempotencyKey, out, p);
        p = copy(SUFFIX, out, p);
        if (p > out.length) {
            throw new IllegalStateException("receipt json buffer overflow estimate=" + estimate + " used=" + p);
        }
        if (p == out.length) {
            return out;
        }
        byte[] trimmed = new byte[p];
        System.arraycopy(out, 0, trimmed, 0, p);
        return trimmed;
    }

    static void writeReceipt(io.undertow.server.HttpServerExchange exchange,
                             HttpJsonCodec jsonCodec,
                             int status,
                             SubmissionReceipt receipt,
                             String submissionRoute,
                             String byKeyRoute) throws IOException {
        if (!jsonCodec.responseGuard(exchange).tryCommit()) {
            return;
        }
        byte[] bytes = toJsonBytes(receipt, submissionRoute, byKeyRoute);
        exchange.setStatusCode(status);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, HttpJsonCodec.JSON_CONTENT_TYPE);
        exchange.getResponseSender().send(java.nio.ByteBuffer.wrap(bytes));
    }

    private static int appendUrlEncoded(String value, byte[] dest, int destPos) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.' || c == '~') {
                dest[destPos++] = (byte) c;
            } else {
                destPos = appendPercentEncoded(c, dest, destPos);
            }
        }
        return destPos;
    }

    private static int appendPercentEncoded(char c, byte[] dest, int destPos) {
        byte[] encoded = java.net.URLEncoder.encode(String.valueOf(c), StandardCharsets.UTF_8).getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(encoded, 0, dest, destPos, encoded.length);
        return destPos + encoded.length;
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

    private static int writeLong(long value, byte[] dest, int destPos) {
        return destPos + writeLongInto(value, dest, destPos);
    }

    private static int writeInt(int value, byte[] dest, int destPos) {
        return writeLong(value, dest, destPos);
    }

    private static int writeBool(boolean value, byte[] dest, int destPos) {
        if (value) {
            dest[destPos] = 't';
            dest[destPos + 1] = 'r';
            dest[destPos + 2] = 'u';
            dest[destPos + 3] = 'e';
            return destPos + 4;
        }
        dest[destPos] = 'f';
        dest[destPos + 1] = 'a';
        dest[destPos + 2] = 'l';
        dest[destPos + 3] = 's';
        dest[destPos + 4] = 'e';
        return destPos + 5;
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
