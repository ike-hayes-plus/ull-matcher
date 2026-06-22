package io.github.ike.ullmatcher.server.cluster;

import io.github.ike.ullmatcher.api.Command;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 支持异步批量复制确认的目标能力。
 */
interface AsyncBatchReplicationTarget {
    CompletableFuture<Void> replicateBatchAsync(List<Command> commands, long timeoutNanos);
}
