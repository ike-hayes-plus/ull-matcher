package io.github.ike.ullmatcher.storage.snapshot;

import io.github.ike.ullmatcher.api.MatchEventHandler;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.core.MatcherConfig;
import io.github.ike.ullmatcher.core.UltraLowLatencyMatcher;
import io.github.ike.ullmatcher.storage.wal.StorageSync;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.nio.file.StandardCopyOption;

/**
 * 订单簿快照存储。
 * <p>
 * 快照内容只保存仍在订单簿中的活跃挂单，不保存已成交/已撤订单。
 * 恢复流程：
 * <ol>
 *     <li>读取最近快照，重建订单簿。</li>
 *     <li>从快照序列号之后重放 WAL。</li>
 * </ol>
 * <p>
 * 注意：
 * 生产环境建议由撮合线程在安全点生成快照，避免并发读订单簿。
 */
public final class SnapshotStore {
    /** 快照文件魔数，四字节字符常量为 {@code SNAP}。 */
    private static final int MAGIC = 0x534E4150; // 魔数：SNAP

    /** 快照二进制格式版本。 */
    private static final int VERSION = 3;

    /**
     * 工具类。
     */
    private SnapshotStore() {}

    /**
     * 为当前活跃订单簿写入原子快照文件。
     *
     * @param file 目标快照文件
     * @param matcher 待快照的撮合器
     * @return 快照元数据
     * @throws IOException 快照无法写入时抛出
     */
    public static SnapshotMetadata write(Path file, UltraLowLatencyMatcher matcher) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        List<SnapshotOrder> orders = new ArrayList<>();
        matcher.forEachLiveOrder(o -> orders.add(new SnapshotOrder(
                o.orderId,
                o.userId,
                o.symbolId,
                o.side,
                o.timeInForce,
                o.price,
                o.quantity,
                o.remaining,
                o.sequence,
                o.expireAtEpochMillis)));
        orders.sort(Comparator.comparingLong(SnapshotOrder::sequence));

        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(matcher.lastSequence());
            out.writeLong(matcher.lastTradeId());
            out.writeLong(orders.size());
            for (SnapshotOrder order : orders) {
                out.writeLong(order.orderId);
                out.writeLong(order.userId);
                out.writeInt(order.symbolId);
                out.writeByte(order.side);
                out.writeByte(order.timeInForce);
                out.writeLong(order.price);
                out.writeLong(order.quantity);
                out.writeLong(order.remaining);
                out.writeLong(order.sequence);
                out.writeLong(order.expireAtEpochMillis);
            }
        }
        StorageSync.forceFile(tmp);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        StorageSync.forceDirectory(file.toAbsolutePath().getParent());
        return new SnapshotMetadata(file, matcher.lastSequence(), matcher.lastTradeId(), orders.size());
    }

    /**
     * 从快照文件恢复撮合器。
     *
     * @param file 快照文件
     * @param cfg 撮合器配置
     * @param handler 恢复后撮合器使用的事件处理器
     * @return 恢复后的撮合器和快照序列号
     * @throws IOException 快照无法读取时抛出
     */
    public static RestoreResult restore(Path file, MatcherConfig cfg, MatchEventHandler handler) throws IOException {
        UltraLowLatencyMatcher matcher = new UltraLowLatencyMatcher(cfg, handler);
        if (!Files.exists(file)) return new RestoreResult(matcher, 0);

        ParsedSnapshot parsed = parse(file);
        for (SnapshotOrder order : parsed.orders()) {
            matcher.restoreLiveOrder(order.orderId, order.userId, order.symbolId, order.side,
                    restoreTimeInForce(order.timeInForce, order.orderId).code,
                    order.price, order.quantity, order.remaining, order.sequence, order.expireAtEpochMillis);
        }
        matcher.restoreSequenceState(parsed.snapshotSequence(), parsed.snapshotTradeSequence());
        return new RestoreResult(matcher, parsed.snapshotSequence());
    }

    public static List<SnapshotLiveOrder> scanLiveOrders(Path file) throws IOException {
        if (!Files.exists(file)) {
            return List.of();
        }
        ParsedSnapshot parsed = parse(file);
        List<SnapshotLiveOrder> orders = new ArrayList<>(parsed.orders().size());
        for (SnapshotOrder order : parsed.orders()) {
            orders.add(new SnapshotLiveOrder(order.orderId, order.symbolId, order.side,
                    restoreTimeInForce(order.timeInForce, order.orderId),
                    order.price, order.quantity, order.remaining, order.sequence, order.expireAtEpochMillis));
        }
        return orders;
    }

    private static ParsedSnapshot parse(Path file) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
            if (in.readInt() != MAGIC) throw new IOException("bad snapshot magic");
            int version = in.readInt();
            if (version != 1 && version != 2 && version != VERSION) throw new IOException("unsupported snapshot version " + version);
            long snapshotSequence = in.readLong();
            long snapshotTradeSequence = version == 1 ? 0L : in.readLong();
            long count = in.readLong();
            List<SnapshotOrder> orders = new ArrayList<>(Math.toIntExact(count));
            for (long i = 0; i < count; i++) {
                SnapshotOrder order = new SnapshotOrder(
                        in.readLong(),
                        in.readLong(),
                        in.readInt(),
                        in.readByte(),
                        in.readByte(),
                        in.readLong(),
                        in.readLong(),
                        in.readLong(),
                        in.readLong(),
                        version >= 3 ? in.readLong() : 0L
                );
                validateSnapshotOrder(order, snapshotSequence);
                orders.add(order);
            }
            return new ParsedSnapshot(snapshotSequence, snapshotTradeSequence, orders);
        }
    }

    private static void validateSnapshotOrder(SnapshotOrder order, long snapshotSequence) throws IOException {
        if (order.remaining <= 0 || order.remaining > order.quantity) {
            throw new IOException("bad snapshot quantity for order " + order.orderId);
        }
        if (order.sequence <= 0 || order.sequence > snapshotSequence) {
            throw new IOException("bad snapshot sequence for order " + order.orderId);
        }
    }

    /**
     * 解码可产生快照挂单的有效期策略子集。
     *
     * @param code 已存储的有效期策略编码
     * @param orderId 订单编号
     * @return 恢复后的有效期策略
     * @throws IOException 编码不是可快照的挂单策略时抛出
     */
    private static TimeInForce restoreTimeInForce(byte code, long orderId) throws IOException {
        if (code == TimeInForce.GTC.code) return TimeInForce.GTC;
        if (code == TimeInForce.POST_ONLY.code) return TimeInForce.POST_ONLY;
        throw new IOException("bad snapshot timeInForce for order " + orderId);
    }

    /**
     * 快照恢复结果。
     *
     * @param matcher 恢复后的撮合器实例
     * @param snapshotSequence 快照包含的最后序列号
     */
    public record RestoreResult(UltraLowLatencyMatcher matcher, long snapshotSequence) {}

    /**
     * 快照写入结果。
     *
     * @param file 快照文件
     * @param lastSequence 快照覆盖的最后命令序列号
     * @param lastTradeId 快照覆盖的最后成交编号
     * @param liveOrderCount 快照中的活跃挂单数量
     */
    public record SnapshotMetadata(Path file, long lastSequence, long lastTradeId, long liveOrderCount) {}

    public record SnapshotLiveOrder(long orderId, int symbolId, byte side, TimeInForce timeInForce,
                                    long price, long quantity, long remaining, long sequence,
                                    long expireAtEpochMillis) {}

    private record ParsedSnapshot(long snapshotSequence, long snapshotTradeSequence, List<SnapshotOrder> orders) {}

    /**
     * 快照文件中的活跃挂单记录。
     *
     * @param orderId 订单编号
     * @param userId 用户编号
     * @param symbolId 交易对或分片编号
     * @param side 方向编码
     * @param timeInForce 有效期策略编码
     * @param price 定点数价格
     * @param quantity 原始定点数数量
     * @param remaining 剩余定点数数量
     * @param sequence 创建该订单的命令序列号
     * @param expireAtEpochMillis 订单绝对过期时间，单位为 epoch millis；非 TTL 订单为 {@code 0}
     */
    private record SnapshotOrder(long orderId, long userId, int symbolId, byte side, byte timeInForce,
                                 long price, long quantity, long remaining, long sequence, long expireAtEpochMillis) {}
}
