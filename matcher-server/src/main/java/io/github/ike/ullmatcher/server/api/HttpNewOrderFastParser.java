package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.TimeInForce;

/**
 * ASCII JSON fast path for {@code POST /api/v1/orders} bodies.
 * <p>
 * Avoids Jackson on the hot path when the payload is a flat object with primitive fields and
 * enum strings, which covers benchmark traffic and most programmatic clients.
 */
final class HttpNewOrderFastParser {
    private HttpNewOrderFastParser() {
    }

    static NewOrderRequest tryParse(byte[] body, int length) {
        if (body == null || length <= 0 || length > body.length || body[0] != '{') {
            return null;
        }
        int i = 1;
        Long userIdBox = null;
        Long orderIdBox = null;
        String side = null;
        String orderType = null;
        String timeInForce = null;
        Long priceBox = null;
        Long quantityBox = null;
        Long ttlMillis = null;
        String idempotencyKey = null;
        String ack = null;
        while (i < length) {
            i = skipWs(body, length, i);
            if (i >= length || body[i] == '}') {
                break;
            }
            if (body[i] == ',') {
                i++;
                continue;
            }
            if (body[i] != '"') {
                return null;
            }
            int keyStart = i + 1;
            int keyEnd = indexOfQuote(body, length, keyStart);
            if (keyEnd < 0) {
                return null;
            }
            String key = asciiString(body, keyStart, keyEnd);
            i = keyEnd + 1;
            i = skipWs(body, length, i);
            if (i >= length || body[i] != ':') {
                return null;
            }
            i++;
            i = skipWs(body, length, i);
            switch (key) {
                case "userId" -> {
                    ParseLong pl = parseLong(body, length, i);
                    if (pl == null) {
                        return null;
                    }
                    userIdBox = pl.value;
                    i = pl.nextIndex;
                }
                case "orderId" -> {
                    ParseLong pl = parseLong(body, length, i);
                    if (pl == null) {
                        return null;
                    }
                    orderIdBox = pl.value;
                    i = pl.nextIndex;
                }
                case "price", "quantity" -> {
                    ParseLong pl = parseLong(body, length, i);
                    if (pl == null) {
                        return null;
                    }
                    if (key.charAt(0) == 'p') {
                        priceBox = pl.value;
                    } else {
                        quantityBox = pl.value;
                    }
                    i = pl.nextIndex;
                }
                case "ttlMillis" -> {
                    ParseLong pl = parseLong(body, length, i);
                    if (pl == null) {
                        return null;
                    }
                    ttlMillis = pl.value;
                    i = pl.nextIndex;
                }
                case "side", "orderType", "timeInForce", "idempotencyKey", "ack" -> {
                    ParseString ps = parseJsonString(body, length, i);
                    if (ps == null) {
                        return null;
                    }
                    switch (key) {
                        case "side" -> side = ps.value;
                        case "orderType" -> orderType = ps.value;
                        case "timeInForce" -> timeInForce = ps.value;
                        case "idempotencyKey" -> idempotencyKey = ps.value;
                        case "ack" -> ack = ps.value;
                        default -> {
                        }
                    }
                    i = ps.nextIndex;
                }
                default -> {
                    i = skipValue(body, length, i);
                    if (i < 0) {
                        return null;
                    }
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
        if (userIdBox == null || orderIdBox == null || side == null || orderType == null || timeInForce == null
                || priceBox == null || quantityBox == null) {
            return null;
        }
        if (!isKnownSide(side) || !isKnownOrderType(orderType) || !isKnownTimeInForce(timeInForce)) {
            return null;
        }
        return new NewOrderRequest(
                userIdBox,
                orderIdBox,
                side,
                orderType,
                timeInForce,
                priceBox,
                quantityBox,
                ttlMillis,
                idempotencyKey,
                ack
        );
    }

    private static boolean isKnownSide(String side) {
        return "BUY".equals(side) || "SELL".equals(side);
    }

    private static boolean isKnownOrderType(String orderType) {
        for (OrderType type : OrderType.values()) {
            if (type.name().equals(orderType)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isKnownTimeInForce(String tif) {
        for (TimeInForce value : TimeInForce.values()) {
            if (value.name().equals(tif)) {
                return true;
            }
        }
        return false;
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

    private static int indexOfQuote(byte[] body, int length, int from) {
        for (int i = from; i < length; i++) {
            if (body[i] == '"') {
                return i;
            }
        }
        return -1;
    }

    private static String asciiString(byte[] body, int start, int end) {
        return new String(body, start, end - start, java.nio.charset.StandardCharsets.US_ASCII);
    }

    private static ParseLong parseLong(byte[] body, int length, int i) {
        if (i >= length) {
            return null;
        }
        boolean negative = false;
        if (body[i] == '-') {
            negative = true;
            i++;
        }
        if (i >= length || body[i] < '0' || body[i] > '9') {
            return null;
        }
        long value = 0L;
        while (i < length) {
            byte b = body[i];
            if (b < '0' || b > '9') {
                break;
            }
            value = value * 10L + (b - '0');
            i++;
        }
        if (negative) {
            value = -value;
        }
        return new ParseLong(value, i);
    }

    private static ParseString parseJsonString(byte[] body, int length, int i) {
        if (i >= length || body[i] != '"') {
            return null;
        }
        int start = i + 1;
        int end = indexOfQuote(body, length, start);
        if (end < 0) {
            return null;
        }
        for (int j = start; j < end; j++) {
            if (body[j] == '\\') {
                return null;
            }
        }
        return new ParseString(asciiString(body, start, end), end + 1);
    }

    private static int skipValue(byte[] body, int length, int i) {
        if (i >= length) {
            return -1;
        }
        byte b = body[i];
        if (b == '"') {
            ParseString ps = parseJsonString(body, length, i);
            return ps == null ? -1 : ps.nextIndex;
        }
        if (b == '{') {
            int depth = 0;
            for (; i < length; i++) {
                if (body[i] == '{') {
                    depth++;
                } else if (body[i] == '}') {
                    depth--;
                    if (depth == 0) {
                        return i + 1;
                    }
                }
            }
            return -1;
        }
        if (b == '[') {
            int depth = 0;
            for (; i < length; i++) {
                if (body[i] == '[') {
                    depth++;
                } else if (body[i] == ']') {
                    depth--;
                    if (depth == 0) {
                        return i + 1;
                    }
                }
            }
            return -1;
        }
        while (i < length) {
            byte c = body[i];
            if (c == ',' || c == '}') {
                return i;
            }
            i++;
        }
        return i;
    }

    private record ParseLong(long value, int nextIndex) {
    }

    private record ParseString(String value, int nextIndex) {
    }
}
