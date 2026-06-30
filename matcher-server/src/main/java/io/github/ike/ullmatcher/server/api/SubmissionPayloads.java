package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.engine.SubmissionReceipt;
import io.github.ike.ullmatcher.server.engine.SubmissionView;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

final class SubmissionPayloads {
    private SubmissionPayloads() {
    }

    static Map<String, Object> fromView(SubmissionView view, String submissionRoute, String byKeyRoute) {
        Map<String, Object> payload = common(
                view.submissionId(),
                view.idempotencyKey(),
                view.operationType(),
                view.userId(),
                view.orderId(),
                view.sequence(),
                view.phase().name(),
                view.localResult() == null ? null : view.localResult().name(),
                view.localDurable(),
                view.replicationRequired(),
                view.replicationCommitted(),
                view.totalTargets(),
                view.requiredAcks(),
                view.ackedTargets(),
                view.retryCount(),
                view.lastError(),
                view.createdAtEpochMillis(),
                view.updatedAtEpochMillis(),
                submissionRoute,
                byKeyRoute
        );
        payload.put("ackedNodeIds", view.ackedNodeIds());
        payload.put("failedNodeIds", view.failedNodeIds());
        payload.put("orderState", view.orderState());
        return payload;
    }

    static Map<String, Object> fromReceipt(SubmissionReceipt receipt, String submissionRoute, String byKeyRoute) {
        return common(
                receipt.submissionId(),
                receipt.idempotencyKey(),
                receipt.operationType(),
                receipt.userId(),
                receipt.orderId(),
                receipt.sequence(),
                receipt.phase().name(),
                receipt.localResult() == null ? null : receipt.localResult().name(),
                receipt.localDurable(),
                receipt.replicationRequired(),
                receipt.replicationCommitted(),
                receipt.totalTargets(),
                receipt.requiredAcks(),
                receipt.ackedTargets(),
                receipt.retryCount(),
                receipt.lastError(),
                receipt.createdAtEpochMillis(),
                receipt.updatedAtEpochMillis(),
                submissionRoute,
                byKeyRoute
        );
    }

    private static Map<String, Object> common(String submissionId,
                                              String idempotencyKey,
                                              String operationType,
                                              long userId,
                                              Long orderId,
                                              long sequence,
                                              String phase,
                                              String localResult,
                                              boolean localDurable,
                                              boolean replicationRequired,
                                              boolean replicationCommitted,
                                              int totalTargets,
                                              int requiredAcks,
                                              int ackedTargets,
                                              long retryCount,
                                              String lastError,
                                              long createdAtEpochMillis,
                                              long updatedAtEpochMillis,
                                              String submissionRoute,
                                              String byKeyRoute) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("submissionId", submissionId);
        payload.put("idempotencyKey", idempotencyKey);
        payload.put("operationType", operationType);
        payload.put("userId", userId);
        payload.put("orderId", orderId);
        payload.put("sequence", sequence);
        payload.put("phase", phase);
        payload.put("localResult", localResult);
        payload.put("result", localResult);
        payload.put("localDurable", localDurable);
        payload.put("replicationRequired", replicationRequired);
        payload.put("replicationCommitted", replicationCommitted);
        payload.put("totalTargets", totalTargets);
        payload.put("requiredAcks", requiredAcks);
        payload.put("ackedTargets", ackedTargets);
        payload.put("retryCount", retryCount);
        payload.put("lastError", lastError);
        payload.put("createdAtEpochMillis", createdAtEpochMillis);
        payload.put("updatedAtEpochMillis", updatedAtEpochMillis);
        payload.put("queryPath", submissionRoute.replace("{submissionId}", submissionId));
        payload.put("queryByIdempotencyPath", byKeyRoute + "?idempotencyKey=" + URLEncoder.encode(idempotencyKey, StandardCharsets.UTF_8));
        return payload;
    }
}
