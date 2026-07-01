package io.github.ike.ullmatcher.hft;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.ring.SpscRingBuffer;
import io.github.ike.ullmatcher.storage.wal.WalWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 带 WAL 网关提交语义测试。
 */
class JournaledMatcherGatewayTest {
    /** 测试交易对编号。 */
    private static final int SYMBOL = 1001;

    /**
     * 验证环形缓冲区满时返回明确结果，且调用方知道命令已经写入 WAL。
     *
     * @throws Exception WAL 写入失败时抛出
     */
    @Test
    void returnsRingFullAfterWalAppendWhenOfferTimesOut() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(1);
        assertTrue(ring.offer(Command.snapshotMarker(1, SYMBOL)));
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(wal, ring, 1, 0, () -> true);

        Command command = newOrder(2);
        SubmitResult result = gateway.trySubmit(command, 0);

        assertEquals(SubmitResult.RING_FULL_BEFORE_WAL_APPEND, result);
        assertFalse(result.walAppended());
        assertTrue(wal.commands.isEmpty());
        assertEquals(0, gateway.walAppendCount());
        assertEquals(0, gateway.walForceCount());
        assertEquals(0, wal.forceCount);
    }

    /**
     * 验证撮合循环不可用时不会先写 WAL。
     *
     * @throws Exception WAL 写入失败时抛出
     */
    @Test
    void doesNotAppendWalWhenMatcherIsNotAccepting() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(1);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(wal, ring, 1, 0, () -> false);

        SubmitResult result = gateway.trySubmit(newOrder(1), 0);

        assertEquals(SubmitResult.MATCHER_NOT_RUNNING, result);
        assertFalse(result.walAppended());
        assertTrue(wal.commands.isEmpty());
        assertEquals(0, wal.forceCount);
        assertEquals(1, gateway.failedBeforeWalCount());
    }

    @Test
    void batchDurabilityForcesOnConfiguredThreshold() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(8);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                wal, ring, 1, 0, () -> true, WalDurabilityMode.SYNC_PER_BATCH, 2, 0L
        );

        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmit(newOrder(1), 0));
        assertEquals(0, wal.forceCount);
        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmit(newOrder(2), 0));
        assertEquals(1, wal.forceCount);
        assertEquals(1, gateway.walForceCount());
    }

    @Test
    void osBufferedModeSkipsForceOnSubmit() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(8);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                wal, ring, 1, 0, () -> true, WalDurabilityMode.OS_BUFFERED, 1, 0L
        );

        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmit(newOrder(1), 0));
        assertEquals(0, wal.forceCount);
        gateway.flushWal();
        assertEquals(1, wal.forceCount);
    }

    @Test
    void batchDurabilityForcesOnConfiguredDelayWindow() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(8);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                wal, ring, 1, 0, () -> true, WalDurabilityMode.SYNC_PER_BATCH, 32, 500L
        );

        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmit(newOrder(1), 0));
        assertEquals(0, wal.forceCount);
        Thread.sleep(2L);
        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmit(newOrder(2), 0));
        assertEquals(1, wal.forceCount);
    }

    @Test
    void batchSubmitAppendsAndPublishesWholeBatch() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(8);
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                wal, ring, 1, 0, () -> true, WalDurabilityMode.SYNC_PER_BATCH, 4, 0L
        );

        List<Command> batch = List.of(newOrder(1), newOrder(2), newOrder(3));
        assertEquals(SubmitResult.ACCEPTED, gateway.trySubmitBatch(batch, 0));
        assertEquals(batch, wal.commands);
        assertEquals(3, gateway.acceptedCount());
        assertEquals(3, gateway.walAppendCount());
        assertEquals(0, wal.forceCount);
        assertEquals(batch.get(0), ring.poll());
        assertEquals(batch.get(1), ring.poll());
        assertEquals(batch.get(2), ring.poll());
    }

    @Test
    void batchSubmitDoesNotPartiallyAppendWhenOnlyPartOfRingHasCapacity() throws Exception {
        InMemoryWal wal = new InMemoryWal();
        SpscRingBuffer<Command> ring = new SpscRingBuffer<>(4);
        assertTrue(ring.offer(Command.snapshotMarker(99, SYMBOL)));
        assertTrue(ring.offer(Command.snapshotMarker(100, SYMBOL)));
        JournaledMatcherGateway gateway = new JournaledMatcherGateway(
                wal, ring, 1, 0, () -> true, WalDurabilityMode.SYNC_PER_COMMAND, 1, 0L
        );

        List<Command> batch = List.of(newOrder(1), newOrder(2), newOrder(3));
        assertEquals(SubmitResult.RING_FULL_BEFORE_WAL_APPEND, gateway.trySubmitBatch(batch, 0));

        assertTrue(wal.commands.isEmpty());
        assertEquals(0, gateway.acceptedCount());
        assertEquals(0, gateway.walAppendCount());
        assertEquals(0, gateway.walForceCount());
        assertEquals(2, ring.size());
    }

    /**
     * 创建测试新订单。
     *
     * @param sequence 序列号
     * @return 新订单命令
     */
    private static Command newOrder(long sequence) {
        return Command.newOrder(sequence, 100 + sequence, 10, SYMBOL, Side.SELL,
                OrderType.LIMIT, TimeInForce.GTC, 100, 1);
    }

    /**
     * 内存 WAL 测试替身。
     */
    private static final class InMemoryWal implements WalWriter {
        /** 已追加命令。 */
        private final List<Command> commands = new ArrayList<>();
        private int forceCount;

        /**
         * 记录命令。
         *
         * @param command 待持久化命令
         */
        @Override
        public void append(Command command) {
            commands.add(command);
        }

        /**
         * 内存替身无需刷盘。
         */
        @Override
        public void force() {
            forceCount++;
        }

        /**
         * 内存替身无需关闭。
         *
         * @throws IOException 不会抛出
         */
        @Override
        public void close() throws IOException {}
    }
}
