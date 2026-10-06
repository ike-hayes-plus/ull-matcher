package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;

/**
 * ASCII enum parsing for hot REST submit paths (avoids trim/toUpperCase + {@link Enum#valueOf}).
 */
final class HttpOrderEnums {
    private HttpOrderEnums() {
    }

    static Side parseSide(String raw) {
        if (raw == null) {
            throw new BadRequestException("missing side");
        }
        return switch (raw) {
            case "BUY" -> Side.BUY;
            case "SELL" -> Side.SELL;
            default -> throw invalid("side", raw);
        };
    }

    static OrderType parseOrderType(String raw) {
        if (raw == null) {
            throw new BadRequestException("missing orderType");
        }
        return switch (raw) {
            case "LIMIT" -> OrderType.LIMIT;
            case "MARKET_WITH_PROTECTION" -> OrderType.MARKET_WITH_PROTECTION;
            default -> throw invalid("orderType", raw);
        };
    }

    static TimeInForce parseTimeInForce(String raw) {
        if (raw == null) {
            throw new BadRequestException("missing timeInForce");
        }
        return switch (raw) {
            case "GTC" -> TimeInForce.GTC;
            case "IOC" -> TimeInForce.IOC;
            case "FOK" -> TimeInForce.FOK;
            case "POST_ONLY" -> TimeInForce.POST_ONLY;
            default -> throw invalid("timeInForce", raw);
        };
    }

    private static BadRequestException invalid(String field, String raw) {
        return new BadRequestException("invalid " + field + ": " + raw);
    }
}
