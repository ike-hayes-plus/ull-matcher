package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.server.engine.SubmissionPhase;
import io.github.ike.ullmatcher.server.engine.SubmissionReceipt;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HttpSubmissionReceiptWriterTest {
    private static final String ROUTE_SUBMISSION = "/api/v1/submissions/{submissionId}";
    private static final String ROUTE_BY_KEY = "/api/v1/submissions/by-idempotency";

    @Test
    void matchesJacksonPayloadShape() throws Exception {
        SubmissionReceipt receipt = new SubmissionReceipt(
                "sub-1",
                "key-1",
                "NEW_ORDER",
                1L,
                99L,
                7L,
                SubmissionPhase.COMMITTED,
                SubmitResult.ACCEPTED,
                true,
                true,
                true,
                2,
                1,
                1,
                0L,
                null,
                1000L,
                1001L
        );
        byte[] fast = HttpSubmissionReceiptWriter.toJsonBytes(receipt, ROUTE_SUBMISSION, ROUTE_BY_KEY);
        Map<?, ?> expected = SubmissionPayloads.fromReceipt(receipt, ROUTE_SUBMISSION, ROUTE_BY_KEY);
        JsonMapper mapper = JsonMapper.builder().build();
        byte[] jackson = mapper.writeValueAsBytes(expected);
        assertEquals(mapper.readTree(jackson), mapper.readTree(fast));
    }

    @Test
    void nullIdempotencyKeyEncodesAsNull() {
        SubmissionReceipt receipt = new SubmissionReceipt(
                "sub-2",
                null,
                "NEW_ORDER",
                1L,
                99L,
                7L,
                SubmissionPhase.COMMITTED,
                SubmitResult.ACCEPTED,
                true,
                true,
                true,
                2,
                1,
                1,
                0L,
                null,
                1000L,
                1001L
        );
        byte[] fast = HttpSubmissionReceiptWriter.toJsonBytes(receipt, ROUTE_SUBMISSION, ROUTE_BY_KEY);
        String json = new String(fast, StandardCharsets.UTF_8);
        org.junit.jupiter.api.Assertions.assertTrue(json.contains("\"idempotencyKey\":null"));
    }
}
