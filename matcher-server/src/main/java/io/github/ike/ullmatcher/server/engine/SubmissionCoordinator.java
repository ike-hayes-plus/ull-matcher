package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.CommandPool;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.server.bootstrap.MatcherServerConfig;

import java.io.IOException;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

final class SubmissionCoordinator {
    private final MatcherServerConfig config;
    private final CommandPool commandPool;
    private final AtomicLong nextSequence;
    private final TtlCancelGuard ttlCancelGuard;
    private final OrderStateTracker orderStateTracker;

    SubmissionCoordinator(MatcherServerConfig config,
                          CommandPool commandPool,
                          AtomicLong nextSequence,
                          TtlCancelGuard ttlCancelGuard,
                          OrderStateTracker orderStateTracker) {
        this.config = Objects.requireNonNull(config, "config");
        this.commandPool = Objects.requireNonNull(commandPool, "commandPool");
        this.nextSequence = Objects.requireNonNull(nextSequence, "nextSequence");
        this.ttlCancelGuard = Objects.requireNonNull(ttlCancelGuard, "ttlCancelGuard");
        this.orderStateTracker = Objects.requireNonNull(orderStateTracker, "orderStateTracker");
    }

    MatcherNodeService.SubmitResponse submitNewOrder(MatcherEngine current,
                                                     long userId,
                                                     long orderId,
                                                     Side side,
                                                     OrderType orderType,
                                                     TimeInForce tif,
                                                     long price,
                                                     long quantity,
                                                     Long ttlMillis) throws IOException {
        return submit(current, new SubmissionRequest.NewOrderRequest(userId, orderId, side, orderType, tif, price, quantity, ttlMillis));
    }

    MatcherNodeService.SubmitResponse cancelOrder(MatcherEngine current, long orderId) throws IOException {
        return submit(current, new SubmissionRequest.CancelOrderRequest(orderId));
    }

    BatchSubmitOutcome submitBatch(MatcherEngine current,
                                   List<SubmissionRequest> requests,
                                   BatchSubmitContext context) throws IOException {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(requests, "requests");
        Objects.requireNonNull(context, "context");
        context.clear();
        if (requests.isEmpty()) {
            return BatchSubmitOutcome.EMPTY;
        }
        if (!current.runtime().acceptsClientCommands()) {
            context.result = SubmitResult.MATCHER_NOT_RUNNING;
            return new BatchSubmitOutcome(0, false, 0, 0);
        }
        long nowEpochMillis = System.currentTimeMillis();
        try {
            for (SubmissionRequest request : requests) {
                SubmissionRequest.PreparedSubmission item = request.prepare(
                        config,
                        commandPool,
                        nextSequence,
                        ttlCancelGuard,
                        orderStateTracker,
                        nowEpochMillis
                );
                context.addPrepared(item);
            }
        } catch (SubmissionRequest.CommandPoolExhaustedException exhausted) {
            releasePrepared(context.prepared, context.preparedCount);
            context.clear();
            context.result = SubmitResult.COMMAND_POOL_EXHAUSTED;
            return new BatchSubmitOutcome(0, false, 0, 0);
        }
        SubmitResult result;
        try {
            result = current.gateway().trySubmitBatch(context.preparedView, config.gatewayOfferTimeoutNanos());
        } catch (IOException e) {
            releasePrepared(context.prepared, context.preparedCount);
            context.clear();
            throw e;
        }
        for (int i = 0; i < context.preparedCount; i++) {
            SubmissionRequest.PreparedSubmission item = context.prepared[i];
            if (result == SubmitResult.ACCEPTED) {
                item.acceptedAction().run();
            } else {
                item.command().release();
            }
        }
        context.result = result;
        return new BatchSubmitOutcome(context.preparedCount, true, 0, 0);
    }

    private MatcherNodeService.SubmitResponse submit(MatcherEngine current, SubmissionRequest request) throws IOException {
        BatchSubmitContext context = new BatchSubmitContext(1);
        BatchSubmitOutcome outcome = submitBatch(current, List.of(request), context);
        long sequence = outcome.preparedCount() == 0 ? 0L : context.prepared[0].sequence();
        return new MatcherNodeService.SubmitResponse(context.result, sequence);
    }

    record BatchSubmitOutcome(int preparedCount,
                              boolean replicationRequired,
                              int requiredAcks,
                              int totalTargets) {
        private static final BatchSubmitOutcome EMPTY = new BatchSubmitOutcome(0, false, 0, 0);
    }

    static final class BatchSubmitContext {
        final SubmissionRequest.PreparedSubmission[] prepared;
        final PreparedCommandView preparedView;
        int preparedCount;
        SubmitResult result;

        BatchSubmitContext(int capacity) {
            this.prepared = new SubmissionRequest.PreparedSubmission[capacity];
            this.preparedView = new PreparedCommandView(this);
        }

        void clear() {
            for (int i = 0; i < preparedCount; i++) {
                prepared[i] = null;
            }
            preparedCount = 0;
            result = SubmitResult.ACCEPTED;
        }

        void addPrepared(SubmissionRequest.PreparedSubmission preparedSubmission) {
            prepared[preparedCount++] = preparedSubmission;
        }
    }

    private static void releasePrepared(SubmissionRequest.PreparedSubmission[] prepared, int size) {
        for (int i = 0; i < size; i++) {
            SubmissionRequest.PreparedSubmission item = prepared[i];
            item.command().release();
            prepared[i] = null;
        }
    }

    private static final class PreparedCommandView extends AbstractList<Command> {
        private final BatchSubmitContext context;

        private PreparedCommandView(BatchSubmitContext context) {
            this.context = context;
        }

        @Override
        public Command get(int index) {
            if (index < 0 || index >= context.preparedCount) {
                throw new IndexOutOfBoundsException(index);
            }
            return context.prepared[index].command();
        }

        @Override
        public int size() {
            return context.preparedCount;
        }
    }
}
