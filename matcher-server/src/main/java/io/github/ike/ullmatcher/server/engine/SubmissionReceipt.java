package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.hft.SubmitResult;

/**
 * 提交接口的即时回执视图。
 */
public record SubmissionReceipt(
        String submissionId,
        String idempotencyKey,
        String operationType,
        Long userId,
        long orderId,
        long sequence,
        SubmissionPhase phase,
        SubmitResult localResult,
        boolean localDurable,
        boolean replicationRequired,
        boolean replicationCommitted,
        int totalTargets,
        int requiredAcks,
        int ackedTargets,
        long retryCount,
        String lastError,
        long createdAtEpochMillis,
        long updatedAtEpochMillis
) {
}
