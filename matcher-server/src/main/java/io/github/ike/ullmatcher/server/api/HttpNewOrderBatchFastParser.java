package io.github.ike.ullmatcher.server.api;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Fast path for {@code POST /api/v1/orders/batch} when payload is {@code {"orders":[...],"ack":?}}.
 */
final class HttpNewOrderBatchFastParser {
    private HttpNewOrderBatchFastParser() {
    }

    static NewOrderBatchRequest tryParse(byte[] body, int length) {
        if (body == null || length <= 12 || length > body.length || body[0] != '{') {
            return null;
        }
        byte[] ordersKey = "\"orders\"".getBytes(StandardCharsets.US_ASCII);
        int ordersIdx = indexOf(body, length, ordersKey, 1);
        if (ordersIdx < 0) {
            return null;
        }
        int i = ordersIdx + ordersKey.length;
        i = skipWs(body, length, i);
        if (i >= length || body[i] != ':') {
            return null;
        }
        i++;
        i = skipWs(body, length, i);
        if (i >= length || body[i] != '[') {
            return null;
        }
        i++;
        ArrayList<NewOrderRequest> orders = new ArrayList<>();
        while (i < length) {
            i = skipWs(body, length, i);
            if (i >= length) {
                return null;
            }
            if (body[i] == ']') {
                i++;
                break;
            }
            if (body[i] != '{') {
                return null;
            }
            int objectStart = i;
            int objectEnd = matchingBrace(body, length, objectStart);
            if (objectEnd < 0) {
                return null;
            }
            int objectLength = objectEnd - objectStart + 1;
            byte[] slice = java.util.Arrays.copyOfRange(body, objectStart, objectStart + objectLength);
            NewOrderRequest order = HttpNewOrderFastParser.tryParse(slice, objectLength);
            if (order == null) {
                return null;
            }
            orders.add(order);
            i = objectEnd + 1;
            i = skipWs(body, length, i);
            if (i < length && body[i] == ',') {
                i++;
            }
        }
        if (orders.isEmpty()) {
            return null;
        }
        String ack = null;
        i = skipWs(body, length, i);
        if (i < length && body[i] == ',') {
            i++;
            i = skipWs(body, length, i);
            if (i + 6 < length && body[i] == '"') {
                byte[] ackKey = "\"ack\"".getBytes(StandardCharsets.US_ASCII);
                if (matchesAt(body, length, i, ackKey)) {
                    i += ackKey.length;
                    i = skipWs(body, length, i);
                    if (i < length && body[i] == ':') {
                        i++;
                        i = skipWs(body, length, i);
                        if (i < length && body[i] == '"') {
                            int start = i + 1;
                            int end = indexOfQuote(body, length, start);
                            if (end < 0) {
                                return null;
                            }
                            ack = new String(body, start, end - start, StandardCharsets.US_ASCII);
                            i = end + 1;
                        }
                    }
                } else {
                    return null;
                }
            }
        }
        i = skipWs(body, length, i);
        if (i >= length || body[i] != '}') {
            return null;
        }
        i++;
        i = skipWs(body, length, i);
        if (i != length) {
            return null;
        }
        return new NewOrderBatchRequest(List.copyOf(orders), ack);
    }

    private static int matchingBrace(byte[] body, int length, int start) {
        if (body[start] != '{') {
            return -1;
        }
        int depth = 0;
        for (int i = start; i < length; i++) {
            byte b = body[i];
            if (b == '{') {
                depth++;
            } else if (b == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static boolean matchesAt(byte[] body, int length, int offset, byte[] needle) {
        if (offset + needle.length > length) {
            return false;
        }
        for (int j = 0; j < needle.length; j++) {
            if (body[offset + j] != needle[j]) {
                return false;
            }
        }
        return true;
    }

    private static int indexOf(byte[] body, int length, byte[] needle, int from) {
        outer:
        for (int i = from; i <= length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (body[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static int indexOfQuote(byte[] body, int length, int from) {
        for (int i = from; i < length; i++) {
            if (body[i] == '"') {
                return i;
            }
        }
        return -1;
    }

    private static int skipWs(byte[] body, int length, int i) {
        while (i < length) {
            byte b = body[i];
            if (b != ' ' && b != '\n' && b != '\r' && b != '\t') {
                return i;
            }
            i++;
        }
        return i;
    }
}
