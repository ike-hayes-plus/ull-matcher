package io.github.ike.ullmatcher.ha.coordination;

import io.github.ike.ullmatcher.runtime.MatchLoop;
import io.github.ike.ullmatcher.runtime.MatchLoopSnapshot;

import java.util.Objects;

/**
 * HA 控制面与本地撮合循环之间的装配层。
 */
public final class HaMatchRuntime {
    private final String nodeId;
    private final MatchLoop loop;

    private volatile HaRole role;
    private volatile FencingToken fencingToken;

    public HaMatchRuntime(String nodeId, MatchLoop loop, HaRole initialRole, FencingToken fencingToken) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.loop = Objects.requireNonNull(loop, "loop");
        this.role = Objects.requireNonNull(initialRole, "initialRole");
        this.fencingToken = Objects.requireNonNull(fencingToken, "fencingToken");
        applyRole(initialRole);
    }

    public String nodeId() {
        return nodeId;
    }

    public HaRole role() {
        return role;
    }

    public FencingToken fencingToken() {
        return fencingToken;
    }

    public boolean acceptsClientCommands() {
        return role == HaRole.PRIMARY && loop.isAcceptingCommands();
    }

    public void promote(FencingToken nextToken) {
        if (role == HaRole.FENCED) {
            throw new IllegalStateException("cannot promote fenced runtime");
        }
        loop.activate();
        fencingToken = Objects.requireNonNull(nextToken, "nextToken");
        role = HaRole.PRIMARY;
    }

    public void demoteToStandby() {
        if (role == HaRole.FENCED) {
            throw new IllegalStateException("cannot demote fenced runtime");
        }
        role = HaRole.STANDBY;
        loop.quiesce();
    }

    public void beginCatchUp() {
        if (role == HaRole.FENCED) {
            throw new IllegalStateException("cannot catch up fenced runtime");
        }
        role = HaRole.CATCHING_UP;
        loop.quiesce();
    }

    public void fence() {
        role = HaRole.FENCED;
        loop.stop();
    }

    public MatchLoopSnapshot snapshot() {
        return loop.snapshot();
    }

    private void applyRole(HaRole initialRole) {
        switch (initialRole) {
            case PRIMARY -> loop.activate();
            case STANDBY, CATCHING_UP -> loop.quiesce();
            case FENCED -> loop.stop();
        }
    }
}
