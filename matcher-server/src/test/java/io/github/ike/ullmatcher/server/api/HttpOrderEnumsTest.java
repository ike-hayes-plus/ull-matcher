package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class HttpOrderEnumsTest {
    @Test
    void parsesKnownValues() {
        assertEquals(Side.BUY, HttpOrderEnums.parseSide("BUY"));
        assertEquals(Side.SELL, HttpOrderEnums.parseSide("SELL"));
        assertEquals(OrderType.LIMIT, HttpOrderEnums.parseOrderType("LIMIT"));
        assertEquals(OrderType.MARKET_WITH_PROTECTION, HttpOrderEnums.parseOrderType("MARKET"));
        assertEquals(OrderType.MARKET_WITH_PROTECTION, HttpOrderEnums.parseOrderType("MARKET_WITH_PROTECTION"));
        assertEquals(TimeInForce.GTC, HttpOrderEnums.parseTimeInForce("GTC"));
        assertEquals(TimeInForce.IOC, HttpOrderEnums.parseTimeInForce("IOC"));
        assertEquals(TimeInForce.FOK, HttpOrderEnums.parseTimeInForce("FOK"));
        assertEquals(TimeInForce.POST_ONLY, HttpOrderEnums.parseTimeInForce("POST_ONLY"));
    }

    @Test
    void rejectsMissingAndInvalidValues() {
        assertThrows(BadRequestException.class, () -> HttpOrderEnums.parseSide(null));
        assertThrows(BadRequestException.class, () -> HttpOrderEnums.parseSide("UP"));
        assertThrows(BadRequestException.class, () -> HttpOrderEnums.parseOrderType(null));
        assertThrows(BadRequestException.class, () -> HttpOrderEnums.parseOrderType("STOP"));
        assertThrows(BadRequestException.class, () -> HttpOrderEnums.parseTimeInForce(null));
        assertThrows(BadRequestException.class, () -> HttpOrderEnums.parseTimeInForce("DAY"));
    }
}
