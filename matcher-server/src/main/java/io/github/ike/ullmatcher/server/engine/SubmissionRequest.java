package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandPool;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 提交入口批量聚合后交给单生产者执行的请求模型。
 */
sealed interface SubmissionRequest permits SubmissionRequest.NewOrderRequest, SubmissionRequest.CancelOrderRequest {
    PreparedSubmission prepare(MatcherServerConfig config,
                               CommandPool commandPool,
                               AtomicLong nextSequence,
                               TtlCancelGuard ttlCancelGuard,
                               OrderStateTracker orderStateTracker,
                               long nowEpochMillis);

    record PreparedSubmission(Command command, long sequence, Runnable acceptedAction) {
        public PreparedSubmission {
            Objects.requireNonNull(command, "command");
            Objects.requireNonNull(acceptedAction, "acceptedAction");
        }
    }

    record NewOrderRequest(long userId,
                           long orderId,
                           Side side,
                           OrderType orderType,
                           TimeInForce tif,
                           long price,
                           long quantity,
                           Long ttlMillis) implements SubmissionRequest {
        @Override
        public PreparedSubmission prepare(MatcherServerConfig config,
                                          CommandPool commandPool,
                                          AtomicLong nextSequence,
                                          TtlCancelGuard ttlCancelGuard,
                                          OrderStateTracker orderStateTracker,
                                          long nowEpochMillis) {
            long sequence = nextSequence.getAndIncrement();
            long expireAtEpochMillis = config.ttlCancelConfig()
                    .resolveExpireAtEpochMillis(tif, ttlMillis, nowEpochMillis);
            Command command = commandPool.borrowNewOrder(
                    sequence,
                    orderId,
                    userId,
                    config.matcherConfig().symbolId(),
                    side,
                    orderType,
                    tif,
                    price,
                    quantity,
                    expireAtEpochMillis
            );
            if (command == null) {
                throw new CommandPoolExhaustedException();
            }
            return new PreparedSubmission(
                    command,
                    sequence,
                    () -> {
                        ttlCancelGuard.onSubmissionAccepted(orderId, tif, expireAtEpochMillis);
                        orderStateTracker.onSubmissionAccepted(orderId, side, orderType, tif, price, quantity);
                    }
            );
        }
    }

    record CancelOrderRequest(long orderId) implements SubmissionRequest {
        @Override
        public PreparedSubmission prepare(MatcherServerConfig config,
                                          CommandPool commandPool,
                                          AtomicLong nextSequence,
                                          TtlCancelGuard ttlCancelGuard,
                                          OrderStateTracker orderStateTracker,
                                          long nowEpochMillis) {
            long sequence = nextSequence.getAndIncrement();
            Command command = commandPool.borrowCancel(sequence, orderId, config.matcherConfig().symbolId());
            if (command == null) {
                throw new CommandPoolExhaustedException();
            }
            return new PreparedSubmission(
                    command,
                    sequence,
                    () -> {}
            );
        }
    }

    final class CommandPoolExhaustedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        CommandPoolExhaustedException() {
            super("command pool exhausted");
        }
    }
}
