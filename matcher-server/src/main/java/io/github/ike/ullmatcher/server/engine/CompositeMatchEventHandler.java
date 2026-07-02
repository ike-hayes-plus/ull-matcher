package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.OrderEvent;
import io.github.ike.ullmatcher.api.TtlEvent;
import io.github.ike.ullmatcher.api.TradeEvent;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

final class CompositeMatchEventHandler implements MatchEventHandler {
    private final List<MatchEventHandler> delegates;

    CompositeMatchEventHandler(MatchEventHandler... delegates) {
        this.delegates = Arrays.stream(delegates)
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public void onTrade(TradeEvent event) {
        for (MatchEventHandler delegate : delegates) {
            delegate.onTrade(event);
        }
    }

    @Override
    public void onOrder(OrderEvent event) {
        for (MatchEventHandler delegate : delegates) {
            delegate.onOrder(event);
        }
    }

    @Override
    public void onTtl(TtlEvent event) {
        for (MatchEventHandler delegate : delegates) {
            delegate.onTtl(event);
        }
    }
}
