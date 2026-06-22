package io.github.ike.ullmatcher.ha.replication;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.storage.wal.WalWriter;

import java.io.IOException;
import java.util.Objects;

/**
 * 先写本地 WAL，再复制到 standby 的写入器装配层。
 */
public final class ReplicatedWalWriter implements WalWriter {
    private final WalWriter localWal;
    private final CommandReplicator replicator;
    private final ReplicationMode replicationMode;
    private final long replicationTimeoutNanos;

    public ReplicatedWalWriter(WalWriter localWal,
                               CommandReplicator replicator,
                               ReplicationMode replicationMode,
                               long replicationTimeoutNanos) {
        this.localWal = Objects.requireNonNull(localWal, "localWal");
        this.replicator = Objects.requireNonNull(replicator, "replicator");
        this.replicationMode = Objects.requireNonNull(replicationMode, "replicationMode");
        if (replicationTimeoutNanos < 0L) {
            throw new IllegalArgumentException("replicationTimeoutNanos must be non-negative");
        }
        this.replicationTimeoutNanos = replicationTimeoutNanos;
    }

    @Override
    public void append(Command command) throws IOException {
        localWal.append(command);
        ReplicationResult result = replicator.replicate(command, replicationTimeoutNanos);
        if (!result.satisfies(replicationMode)) {
            throw new IOException("replication did not satisfy mode " + replicationMode + ": " + result);
        }
    }

    @Override
    public void force() throws IOException {
        localWal.force();
    }

    @Override
    public void close() throws IOException {
        localWal.close();
    }
}
