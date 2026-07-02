package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;

final class SubmissionFingerprints {
    private static final long OPERATION_HASH_SEED = 0xD6E8FEB86659FD93L;

    private SubmissionFingerprints() {}

    static SubmissionTracker.RequestFingerprint newOrder(long userId,
                                                          long orderId,
                                                          Side side,
                                                          OrderType orderType,
                                                          TimeInForce tif,
                                                          long price,
                                                          long quantity,
                                                          Long ttlMillis) {
        long hash = OPERATION_HASH_SEED;
        hash = SubmissionTracker.mix(hash, 1L);
        hash = SubmissionTracker.mix(hash, userId);
        hash = SubmissionTracker.mix(hash, orderId);
        hash = SubmissionTracker.mix(hash, side.code);
        hash = SubmissionTracker.mix(hash, orderType.code);
        hash = SubmissionTracker.mix(hash, tif.code);
        hash = SubmissionTracker.mix(hash, price);
        hash = SubmissionTracker.mix(hash, quantity);
        long ttlFingerprint = ttlMillis == null ? Long.MIN_VALUE : ttlMillis;
        hash = SubmissionTracker.mix(hash, ttlFingerprint);
        return new SubmissionTracker.RequestFingerprint(
                hash,
                side.code,
                orderType.code,
                tif.code,
                price,
                quantity,
                ttlFingerprint
        );
    }

    static SubmissionTracker.RequestFingerprint cancel(long orderId) {
        long hash = OPERATION_HASH_SEED;
        hash = SubmissionTracker.mix(hash, 2L);
        return SubmissionTracker.RequestFingerprint.generic(SubmissionTracker.mix(hash, orderId));
    }
}
