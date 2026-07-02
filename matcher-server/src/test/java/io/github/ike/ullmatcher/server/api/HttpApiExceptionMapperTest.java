package io.github.ike.ullmatcher.server.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

final class HttpApiExceptionMapperTest {
    @Test
    void classifiesApiExceptionsByType() {
        ServerApiException badRequest = HttpApiExceptionMapper.map("submit order", new IllegalArgumentException("bad input"));
        assertInstanceOf(BadRequestException.class, badRequest);
        assertEquals(400, badRequest.statusCode());
        assertEquals("bad_request", badRequest.errorCode());

        ServerApiException conflict = HttpApiExceptionMapper.map("submit order", new IllegalStateException("matcher stopped"));
        assertInstanceOf(ConflictException.class, conflict);
        assertEquals(409, conflict.statusCode());
        assertEquals("conflict", conflict.errorCode());

        ServerApiException unavailable = HttpApiExceptionMapper.map("submit order", new ServiceUnavailableException("ring full"));
        assertInstanceOf(ServiceUnavailableException.class, unavailable);
        assertEquals(503, unavailable.statusCode());
        assertEquals("service_unavailable", unavailable.errorCode());

        ServerApiException overloaded = HttpApiExceptionMapper.map("runtime readiness", new OverloadedException("too many requests"));
        assertInstanceOf(OverloadedException.class, overloaded);
        assertEquals(503, overloaded.statusCode());
        assertEquals("overloaded", overloaded.errorCode());

        ServerApiException timeout = HttpApiExceptionMapper.map("runtime readiness", new RequestTimeoutException("request exceeded timeout"));
        assertInstanceOf(RequestTimeoutException.class, timeout);
        assertEquals(503, timeout.statusCode());
        assertEquals("request_timeout", timeout.errorCode());

        ServerApiException internal = HttpApiExceptionMapper.map("submit order", new RuntimeException("boom"));
        assertInstanceOf(InternalServerException.class, internal);
        assertEquals(500, internal.statusCode());
        assertEquals("internal_error", internal.errorCode());
    }
}
