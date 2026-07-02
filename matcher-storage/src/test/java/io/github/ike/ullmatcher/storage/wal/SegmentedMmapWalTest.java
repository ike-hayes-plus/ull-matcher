package io.github.ike.ullmatcher.storage.wal;

import io.github.ike.ullmatcher.api.*;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.storage.replay.ReplayService;
import io.github.ike.ullmatcher.storage.snapshot.SnapshotStore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分段 WAL 滚动、清理和恢复测试。
 */
class SegmentedMmapWalTest {
    /** 测试交易对编号。 */
    private static final int SYMBOL = 1001;

    /** 临时目录。 */
    @TempDir
    Path tempDir;

    /**
     * 验证 WAL 写满后会自动滚动，并且可以跨分段顺序重放。
     */
    @Test
    void rotatesAndReplaysAllSegments() throws Exception {
        try (SegmentedMmapWal wal = new SegmentedMmapWal(tempDir, "symbol-1", MmapCommandWal.RECORD_SIZE * 2L)) {
            for (long seq = 1; seq <= 5; seq++) {
                wal.append(newSell(seq, seq, 100 + seq, 1));
            }
            wal.force();

            assertEquals(3, wal.segments().size());

            wal.resetReader();
            List<Long> sequences = new ArrayList<>();
            Command command;
            while ((command = wal.next()) != null) {
                sequences.add(command.sequence);
            }

            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), sequences);
        }
    }

    @Test
    void appendAllRotatesAndReplaysAllSegments() throws Exception {
        try (SegmentedMmapWal wal = new SegmentedMmapWal(tempDir, "symbol-1", MmapCommandWal.RECORD_SIZE * 2L)) {
            wal.appendAll(List.of(
                    newSell(1, 1, 101, 1),
                    newSell(2, 2, 102, 1),
                    newSell(3, 3, 103, 1),
                    newSell(4, 4, 104, 1),
                    newSell(5, 5, 105, 1)
            ));
            wal.force();

            assertEquals(3, wal.segments().size());

            wal.resetReader();
            List<Long> sequences = new ArrayList<>();
            Command command;
            while ((command = wal.next()) != null) {
                sequences.add(command.sequence);
            }

            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), sequences);
        }
    }

    /**
     * 验证快照覆盖的旧 WAL 分段会先归档再删除。
     */
    @Test
    void deletesSnapshotCoveredSegmentsAfterArchive() throws Exception {
        List<Path> archived = new ArrayList<>();
        try (SegmentedMmapWal wal = new SegmentedMmapWal(
                tempDir, "symbol-1", MmapCommandWal.RECORD_SIZE * 2L, archived::add)) {
            for (long seq = 1; seq <= 6; seq++) {
                wal.append(newSell(seq, seq, 100 + seq, 1));
            }
            wal.force();

            WalRetentionResult result = wal.deleteSegmentsCoveredBySnapshot(4);

            assertEquals(2, result.archivedSegments().size());
            assertEquals(2, result.deletedSegments().size());
            assertEquals(archived, result.archivedSegments());
            assertEquals(1, wal.segments().size());
            assertTrue(Files.exists(wal.currentSegmentPath()));
        }
    }

    /**
     * 验证最近快照加快照后的 WAL 可以恢复订单簿和成交编号状态。
     */
    @Test
    void restoresFromSnapshotAndReplaysSegmentedWal() throws Exception {
        MatcherConfig cfg = new MatcherConfig(SYMBOL, 1024, 4096, 4096, 100_000_000L, true);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(cfg, new NoopHandler());
        Path walDir = tempDir.resolve("wal");
        Path snapshot = tempDir.resolve("snapshot/symbol-1.snap");

        try (SegmentedMmapWal wal = new SegmentedMmapWal(walDir, "symbol-1", MmapCommandWal.RECORD_SIZE * 2L)) {
            apply(wal, matcher, newSell(1, 101, 100, 10));
            apply(wal, matcher, newBuy(2, 201, 100, 5));
            SnapshotStore.write(snapshot, matcher);
            apply(wal, matcher, newBuy(3, 202, 100, 2));
            wal.force();

            SnapshotStore.RestoreResult result = SnapshotStore.restore(snapshot, cfg, new NoopHandler());
            UltraLowLatencyMatcher restored = result.matcher();

            wal.resetReader();
            long applied = ReplayService.replay(wal, restored, result.snapshotSequence());

            assertEquals(1, applied);
            assertEquals(3, restored.lastSequence());
            assertEquals(2, restored.lastTradeId());
            assertEquals(1, restored.liveOrderCount());
        }
    }

    /**
     * 验证快照 checkpoint 清单会记录 WAL 分段并能校验本地文件。
     */
    @Test
    void writesAndValidatesWalManifest() throws Exception {
        MatcherConfig cfg = new MatcherConfig(SYMBOL, 1024, 4096, 4096, 100_000_000L, true);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(cfg, new NoopHandler());
        Path walDir = tempDir.resolve("wal");
        Path snapshot = tempDir.resolve("snapshot/symbol-1.snap");

        try (SegmentedMmapWal wal = new SegmentedMmapWal(walDir, "symbol-1", MmapCommandWal.RECORD_SIZE * 2L)) {
            apply(wal, matcher, newSell(1, 101, 100, 10));
            apply(wal, matcher, newSell(2, 102, 101, 10));
            SnapshotStore.SnapshotMetadata metadata = SnapshotStore.write(snapshot, matcher);

            WalManifest manifest = wal.writeManifest(metadata.file(), metadata.lastSequence(), metadata.lastTradeId());
            WalManifest restored = wal.readManifest();

            assertEquals(manifest.snapshotSequence(), restored.snapshotSequence());
            assertEquals(2, restored.snapshotSequence());
            assertFalse(restored.segments().isEmpty());
            restored.validateSegments();
        }
    }

    /**
     * 验证快照清单引用的 WAL 分段缺失时会显式失败。
     */
    @Test
    @Tag("chaos")
    void manifestValidationFailsWhenReferencedSegmentIsMissing() throws Exception {
        MatcherConfig cfg = new MatcherConfig(SYMBOL, 1024, 4096, 4096, 100_000_000L, true);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(cfg, new NoopHandler());
        Path walDir = tempDir.resolve("wal");
        Path snapshot = tempDir.resolve("snapshot/symbol-1.snap");

        try (SegmentedMmapWal wal = new SegmentedMmapWal(walDir, "symbol-1", MmapCommandWal.RECORD_SIZE * 2L)) {
            apply(wal, matcher, newSell(1, 101, 100, 10));
            apply(wal, matcher, newSell(2, 102, 101, 10));
            SnapshotStore.SnapshotMetadata metadata = SnapshotStore.write(snapshot, matcher);

            WalManifest manifest = wal.writeManifest(metadata.file(), metadata.lastSequence(), metadata.lastTradeId());
            Files.delete(manifest.segments().getFirst().path());

            IOException error = assertThrows(IOException.class, manifest::validateSegments);
            assertTrue(error.getMessage().contains("WAL segment missing"));
        }
    }

    /**
     * 验证快照恢复后同价位订单仍按原始 sequence 保持时间优先。
     */
    @Test
    void snapshotRestoreKeepsPriceTimePriority() throws Exception {
        MatcherConfig cfg = new MatcherConfig(SYMBOL, 1024, 4096, 4096, 100_000_000L, true);
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(cfg, new NoopHandler());
        Path snapshot = tempDir.resolve("snapshot/symbol-1.snap");

        matcher.onCommand(Command.newOrder(1, 300, 10, SYMBOL, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100, 1));
        matcher.onCommand(Command.newOrder(2, 100, 11, SYMBOL, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100, 1));
        matcher.onCommand(Command.newOrder(3, 200, 12, SYMBOL, Side.SELL, OrderType.LIMIT, TimeInForce.GTC, 100, 1));
        SnapshotStore.write(snapshot, matcher);

        CapturingHandler handler = new CapturingHandler();
        UltraLowLatencyMatcher restored = SnapshotStore.restore(snapshot, cfg, handler).matcher();
        restored.onCommand(Command.newOrder(4, 999, 99, SYMBOL, Side.BUY, OrderType.LIMIT, TimeInForce.IOC, 100, 1));

        assertEquals(List.of(300L), handler.sellOrderIds);
    }

    /**
     * 验证 WAL 尾部半写记录会被当作崩溃尾部忽略，并可被后续写入覆盖。
     */
    @Test
    @Tag("chaos")
    void ignoresAndOverwritesPartialTailRecord() throws Exception {
        Path walPath = tempDir.resolve("symbol-1.wal");
        long walSize = MmapCommandWal.RECORD_SIZE * 3L;
        try (MmapCommandWal wal = new MmapCommandWal(walPath, walSize)) {
            wal.append(newSell(1, 101, 100, 1));
            wal.force();
        }
        try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(walPath, java.nio.file.StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer partial = java.nio.ByteBuffer.allocate(Integer.BYTES);
            partial.putInt(MmapCommandWal.MAGIC);
            partial.flip();
            channel.write(partial, MmapCommandWal.RECORD_SIZE);
        }

        try (MmapCommandWal wal = new MmapCommandWal(walPath, walSize)) {
            assertEquals(1, wal.next().sequence);
            assertNull(wal.next());
            wal.append(newSell(2, 102, 100, 1));
            wal.resetReader();
            assertEquals(1, wal.next().sequence);
            assertEquals(2, wal.next().sequence);
            assertNull(wal.next());
        }
    }

    /**
     * 写入 WAL 并同步应用到撮合器。
     *
     * @param wal WAL 写入器
     * @param matcher 撮合器
     * @param command 命令
     * @throws Exception WAL 写入失败时抛出
     */
    private static void apply(WalWriter wal, UltraLowLatencyMatcher matcher, Command command) throws Exception {
        wal.append(command);
        matcher.onCommand(command);
    }

    /**
     * 创建测试卖单。
     *
     * @param sequence 序列号
     * @param orderId 订单编号
     * @param price 价格
     * @param quantity 数量
     * @return 新卖单命令
     */
    private static Command newSell(long sequence, long orderId, long price, long quantity) {
        return Command.newOrder(sequence, orderId, 10 + orderId, SYMBOL, Side.SELL,
                OrderType.LIMIT, TimeInForce.GTC, price, quantity);
    }

    /**
     * 创建测试买单。
     *
     * @param sequence 序列号
     * @param orderId 订单编号
     * @param price 价格
     * @param quantity 数量
     * @return 新买单命令
     */
    private static Command newBuy(long sequence, long orderId, long price, long quantity) {
        return Command.newOrder(sequence, orderId, 20 + orderId, SYMBOL, Side.BUY,
                OrderType.LIMIT, TimeInForce.IOC, price, quantity);
    }

    /**
     * 恢复阶段使用的空事件处理器。
     */
    private static final class NoopHandler implements MatchEventHandler {
        /**
         * 忽略成交事件。
         *
         * @param event 成交事件
         */
        @Override
        public void onTrade(TradeEvent event) {}

        /**
         * 忽略订单事件。
         *
         * @param event 订单事件
         */
        @Override
        public void onOrder(OrderEvent event) {}
    }

    /**
     * 捕获成交卖单编号的测试处理器。
     */
    private static final class CapturingHandler implements MatchEventHandler {
        /** 已捕获的卖单编号。 */
        private final List<Long> sellOrderIds = new ArrayList<>();

        /**
         * 捕获成交事件。
         *
         * @param event 成交事件
         */
        @Override
        public void onTrade(TradeEvent event) {
            sellOrderIds.add(event.sellOrderId);
        }

        /**
         * 忽略订单事件。
         *
         * @param event 订单事件
         */
        @Override
        public void onOrder(OrderEvent event) {}
    }
}
