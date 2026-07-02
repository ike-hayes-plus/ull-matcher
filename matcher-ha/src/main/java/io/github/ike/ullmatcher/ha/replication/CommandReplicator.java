package io.github.ike.ullmatcher.ha.replication;

import io.github.ike.ullmatcher.api.Command;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 命令复制器。
 */
public interface CommandReplicator {
    /**
     * 复制一条命令到所有 standby。
     *
     * @param command 待复制命令
     * @param timeoutNanos 本次复制预算
     * @return 复制结果
     * @throws IOException 复制器自身失败时抛出
     */
    ReplicationResult replicate(Command command, long timeoutNanos) throws IOException;

    /**
     * 批量复制多条命令到所有 standby。
     *
     * @param commands 待复制命令，调用方保证顺序已经定好
     * @param timeoutNanos 本次复制预算
     * @return 复制结果
     * @throws IOException 复制器自身失败时抛出
     */
    default ReplicationResult replicateBatch(List<Command> commands, long timeoutNanos) throws IOException {
        if (commands == null || commands.isEmpty()) {
            return new ReplicationResult(0, 0, List.of(), List.of());
        }
        ReplicationResult last = new ReplicationResult(0, 0, List.of(), List.of());
        for (Command command : commands) {
            last = replicate(command, timeoutNanos);
        }
        return last;
    }

    default CompletableFuture<ReplicationResult> replicateBatchAsync(List<Command> commands, long timeoutNanos) throws IOException {
        return CompletableFuture.completedFuture(replicateBatch(commands, timeoutNanos));
    }

    default CompletableFuture<ReplicationResult> replicateBatchAsync(List<Command> commands,
                                                                     ReplicationMode mode,
                                                                     long timeoutNanos) throws IOException {
        return replicateBatchAsync(commands, timeoutNanos);
    }

    /**
     * 复制器建议的单批最大命令数。
     * 协调器可据此收敛批次大小，避免 transport 在多备场景下因批次过胖放大 committed tail。
     */
    default int preferredMaxBatchSize() {
        return 2_048;
    }

    /**
     * 复制器建议的最大并发 in-flight 批次数。
     */
    default int preferredInFlightBatches() {
        return 16;
    }

    /**
     * 复制器建议的批次聚合窗口。
     */
    default long preferredAccumulationNanos() {
        return java.util.concurrent.TimeUnit.MICROSECONDS.toNanos(200);
    }
}
