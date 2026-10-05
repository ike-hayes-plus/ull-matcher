package io.github.ike.ullmatcher.server.engine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Waits for replication committed on a batch of submissions after all are enqueued.
 * <p>
 * Sequential await on one thread: orders in the same ingress batch share one replication
 * commit boundary, so parallel waiting only adds scheduler overhead on the binary hot path.
 */
public final class BatchReplicationAwait {
    private BatchReplicationAwait() {
    }

    public static void awaitCommittedReceipts(SubmissionTracker.SubmissionHandle[] handles,
                                              long timeoutMillis,
                                              SubmissionReceipt[] receipts) throws IOException {
        if (handles.length == 0) {
            return;
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1L, timeoutMillis));
        for (int i = 0; i < handles.length; i++) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                throw new IOException("timed out while waiting for replication committed batch");
            }
            long remainingMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
            receipts[i] = handles[i].awaitCommittedReceipt(remainingMillis);
        }
    }
}
