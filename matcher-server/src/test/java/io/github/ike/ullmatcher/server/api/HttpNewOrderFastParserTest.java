package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class HttpNewOrderFastParserTest {

    @Test
    void parsesBenchmarkCrossingOrder() {
        String json = """
                {"userId":1,"orderId":42,"side":"BUY","orderType":"LIMIT","timeInForce":"IOC","price":101,"quantity":1,"ack":"committed"}
                """;
        NewOrderRequest request = HttpNewOrderFastParser.tryParse(json.getBytes(StandardCharsets.UTF_8), json.length());
        assertNotNull(request);
        assertEquals(1L, request.userId());
        assertEquals(42L, request.orderId());
        assertEquals("BUY", request.side());
        assertEquals("LIMIT", request.orderType());
        assertEquals("IOC", request.timeInForce());
        assertEquals(101L, request.price());
        assertEquals(1L, request.quantity());
        assertEquals("committed", request.ack());
    }

    @Test
    void rejectsEscapedStrings() {
        String json = "{\"userId\":1,\"orderId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":1,\"quantity\":1,\"idempotencyKey\":\"a\\\\b\"}";
        assertNull(HttpNewOrderFastParser.tryParse(json.getBytes(StandardCharsets.UTF_8), json.length()));
    }

    @Test
    void rejectsTruncatedObject() {
        String json = "{\"userId\":1,\"orderId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":100,\"quantity\":1";
        assertNull(HttpNewOrderFastParser.tryParse(json.getBytes(StandardCharsets.UTF_8), json.length()));
    }

    @Test
    void rejectsUnknownEnum() {
        String json = "{\"userId\":1,\"orderId\":1,\"side\":\"UP\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":1,\"quantity\":1}";
        assertNull(HttpNewOrderFastParser.tryParse(json.getBytes(StandardCharsets.UTF_8), json.length()));
    }

    @Test
    void parsesOptionalFieldsAndSkipsUnknownKeys() {
        String json = """
                {"userId":1,"orderId":2,"side":"SELL","orderType":"LIMIT","timeInForce":"GTC","price":3,"quantity":4,"ttlMillis":500,"idempotencyKey":"k1","ack":"local","extra":true}
                """;
        NewOrderRequest request = HttpNewOrderFastParser.tryParse(json.getBytes(StandardCharsets.UTF_8), json.length());
        assertNotNull(request);
        assertEquals(500L, request.ttlMillis());
        assertEquals("k1", request.idempotencyKey());
        assertEquals("local", request.ack());
    }

    @Test
    void rejectsNullBodyAndNonObject() {
        assertNull(HttpNewOrderFastParser.tryParse(null, 0));
        assertNull(HttpNewOrderFastParser.tryParse("[]".getBytes(StandardCharsets.UTF_8), 2));
    }

    @Test
    void rejectsTrailingContentAfterObject() {
        String json = "{\"userId\":1,\"orderId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"timeInForce\":\"GTC\",\"price\":1,\"quantity\":1}x";
        assertNull(HttpNewOrderFastParser.tryParse(json.getBytes(StandardCharsets.UTF_8), json.length()));
    }

    @Test
    void rejectsMissingRequiredFieldsAndInvalidNumbers() {
        assertNull(HttpNewOrderFastParser.tryParse("""
                {"userId":1,"orderId":1,"side":"BUY","orderType":"LIMIT","timeInForce":"GTC","price":1}
                """.getBytes(StandardCharsets.UTF_8), 95));
        assertNull(HttpNewOrderFastParser.tryParse("""
                {"userId":1,"orderId":1,"side":"BUY","orderType":"LIMIT","timeInForce":"GTC","price":-1,"quantity":1}
                """.getBytes(StandardCharsets.UTF_8), 100));
        assertNull(HttpNewOrderFastParser.tryParse("""
                {"userId":x,"orderId":1,"side":"BUY","orderType":"LIMIT","timeInForce":"GTC","price":1,"quantity":1}
                """.getBytes(StandardCharsets.UTF_8), 100));
    }
}
