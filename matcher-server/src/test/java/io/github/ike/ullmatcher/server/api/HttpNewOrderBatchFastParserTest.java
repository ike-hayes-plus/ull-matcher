package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

final class HttpNewOrderBatchFastParserTest {
    private static final String ORDER = """
            {"userId":1,"orderId":42,"side":"BUY","orderType":"LIMIT","timeInForce":"IOC","price":101,"quantity":1}
            """;

    @Test
    void parsesOrdersArray() {
        String json = "{\"orders\":[" + ORDER + "]}";
        NewOrderBatchRequest batch = parse(json);
        assertNotNull(batch);
        assertEquals(1, batch.orders().size());
        assertEquals(42L, batch.orders().getFirst().orderId());
        assertNull(batch.ack());
    }

    @Test
    void parsesAckField() {
        String json = "{\"orders\":[" + ORDER + "],\"ack\":\"committed\"}";
        NewOrderBatchRequest batch = parse(json);
        assertNotNull(batch);
        assertEquals("committed", batch.ack());
    }

    @Test
    void rejectsMissingOrdersKey() {
        assertNull(parse("{\"items\":[]}"));
    }

    @Test
    void rejectsEmptyOrdersArray() {
        assertNull(parse("{\"orders\":[]}"));
    }

    @Test
    void rejectsInvalidOrderObject() {
        assertNull(parse("{\"orders\":[{\"userId\":1}]}"));
    }

    @Test
    void rejectsTrailingContent() {
        assertNull(parse("{\"orders\":[" + ORDER + "]}x"));
    }

    @Test
    void rejectsUnknownFieldAfterOrders() {
        assertNull(parse("{\"orders\":[" + ORDER + "],\"extra\":1}"));
    }

    @Test
    void rejectsUnclosedArray() {
        assertNull(parse("{\"orders\":[" + ORDER));
    }

    @Test
    void rejectsNullBody() {
        assertNull(HttpNewOrderBatchFastParser.tryParse(null, 0));
    }

    @Test
    void rejectsTooShortBody() {
        assertNull(HttpNewOrderBatchFastParser.tryParse("{}".getBytes(StandardCharsets.UTF_8), 2));
    }

    @Test
    void rejectsMalformedAckField() {
        assertNull(parse("{\"orders\":[" + ORDER + "],\"ack\":committed}"));
        assertNull(parse("{\"orders\":[" + ORDER + "],\"nack\":\"x\"}"));
    }

    @Test
    void parsesTwoOrderBatch() {
        String json = "{\"orders\":[" + ORDER + "," + ORDER.replace("42", "43") + "]}";
        NewOrderBatchRequest batch = parse(json);
        assertNotNull(batch);
        assertEquals(2, batch.orders().size());
    }

    private static NewOrderBatchRequest parse(String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return HttpNewOrderBatchFastParser.tryParse(bytes, bytes.length);
    }
}
