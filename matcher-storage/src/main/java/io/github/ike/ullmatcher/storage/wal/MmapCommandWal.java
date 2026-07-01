package io.github.ike.ullmatcher.storage.wal;

import io.github.ike.ullmatcher.api.*;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 固定长度内存映射 WAL。
 * <p>
 * 设计目标：
 * <ol>
 *     <li>避免每条命令一次系统调用；写入先进入内存映射页，由操作系统批量刷盘。</li>
 *     <li>每条记录固定长度，恢复时顺序扫描极快。</li>
 *     <li>不做 JSON 或 Protobuf 编码，避免序列化分配和解析开销。</li>
 * </ol>
 * <p>
 * 注意：
 * <ul>
 *     <li>这是撮合命令日志，不是最终成交账本。</li>
 *     <li>极低延迟场景通常采用异步强制刷盘；强一致场景可按批次或每条强制刷盘。</li>
 *     <li>生产环境可使用 {@link SegmentedMmapWal} 在单文件写满后自动滚动分段。</li>
 * </ul>
 */
public final class MmapCommandWal implements WalWriter, WalReader {
    /** 记录魔数，四字节字符常量为 {@code MWAL}。 */
    public static final int MAGIC = 0x4D57414C; // 魔数：MWAL

    /** 固定 WAL 记录长度，单位为字节。 */
    public static final int RECORD_SIZE = 72;

    /** 缓存的命令类型表，避免重复分配枚举数组。 */
    private static final CommandType[] COMMAND_TYPES = CommandType.values();

    /** 支撑内存映射的文件通道。 */
    private final FileChannel channel;

    /** 内存映射的 WAL 内容。 */
    private final MappedByteBuffer buffer;

    /** 映射容量，单位为字节。 */
    private final long capacity;

    /** 下一次写入偏移。 */
    private long writePosition;

    /** 下一次读取偏移。 */
    private long readPosition;

    /**
     * 打开或创建固定大小的内存映射 WAL 文件。
     *
     * @param path WAL 路径
     * @param fileSizeBytes 映射文件大小，单位为字节
     * @throws IOException 文件无法打开或映射时抛出
     */
    public MmapCommandWal(Path path, long fileSizeBytes) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        if (channel.size() < fileSizeBytes) {
            channel.truncate(fileSizeBytes);
        }
        this.capacity = fileSizeBytes;
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSizeBytes);
        this.writePosition = findEnd();
        this.readPosition = 0;
    }

    /**
     * 追加一条命令。
     * <p>
     * 调用方必须保证单线程写入。
     *
     * @param c 待追加命令
     */
    @Override
    public void append(Command c) {
        if (writePosition + RECORD_SIZE > capacity) {
            throw new IllegalStateException("WAL file is full, rotate WAL file");
        }
        writeRecord(c);
    }

    @Override
    public void appendAll(Iterable<Command> commands) {
        if (commands instanceof java.util.List<Command> list) {
            long bytesRequired = (long) list.size() * RECORD_SIZE;
            if (writePosition + bytesRequired > capacity) {
                throw new IllegalStateException("WAL file is full, rotate WAL file");
            }
            for (Command command : list) {
                writeRecord(command);
            }
            return;
        }
        for (Command command : commands) {
            append(command);
        }
    }

    private void writeRecord(Command c) {
        int p = (int) writePosition;
        buffer.putInt(p, MAGIC);
        buffer.putInt(p + 4, c.type.ordinal());
        buffer.putLong(p + 8, c.sequence);
        buffer.putLong(p + 16, c.orderId);
        buffer.putLong(p + 24, c.userId);
        buffer.putInt(p + 32, c.symbolId);
        buffer.put(p + 36, c.side);
        buffer.put(p + 37, c.orderType);
        buffer.put(p + 38, c.timeInForce);
        buffer.put(p + 39, (byte) 0);
        buffer.putLong(p + 40, c.price);
        buffer.putLong(p + 48, c.quantity);
        buffer.putLong(p + 56, c.expireAtEpochMillis);
        buffer.putLong(p + 64, checksum(c));
        writePosition += RECORD_SIZE;
    }

    /**
     * 顺序读取下一条命令。
     *
     * @return 下一条命令；没有完整记录时返回 {@code null}
     */
    @Override
    public Command next() {
        if (readPosition + RECORD_SIZE > capacity) return null;
        int p = (int) readPosition;
        if (buffer.getInt(p) != MAGIC) return null;

        Command c = readCommand(p);
        if (c == null) {
            if (isTailRecord(readPosition)) return null;
            throw new IllegalStateException("WAL command type mismatch at offset " + readPosition);
        }
        if (checksum(c) != buffer.getLong(p + 64)) {
            if (isTailRecord(readPosition)) return null;
            throw new IllegalStateException("WAL checksum mismatch at offset " + readPosition);
        }
        readPosition += RECORD_SIZE;
        return c;
    }

    /**
     * 刷新内存映射缓冲区。
     */
    @Override
    public void force() throws IOException {
        // WAL segments are fixed-size mapped files. Submit-path durability only
        // needs mapped content flushed; segment creation and directory metadata
        // are synchronized by SegmentedMmapWal when opening a writer.
        buffer.force();
    }

    /**
     * 返回下一次写入偏移。
     *
     * @return 写入位置，单位为字节
     */
    public long writePosition() {
        return writePosition;
    }

    /**
     * 将顺序读取位置重置到 WAL 起始处。
     */
    public void resetReader() {
        readPosition = 0;
    }

    /**
     * 刷新并关闭 WAL 文件。
     *
     * @throws IOException 通道无法关闭时抛出
     */
    @Override
    public void close() throws IOException {
        try {
            force();
        } catch (IOException ignored) {
            // 忽略关闭前刷盘失败。
        }
        channel.close();
    }

    /**
     * 查找第一条未写入记录的偏移。
     *
     * @return 结束偏移
     */
    private long findEnd() {
        long p = 0;
        while (p + RECORD_SIZE <= capacity) {
            if (buffer.getInt((int) p) != MAGIC) return p;
            Command command = readCommand((int) p);
            if (command == null || checksum(command) != buffer.getLong((int) p + 64)) {
                if (isTailRecord(p)) return p;
                throw new IllegalStateException("WAL corruption at offset " + p);
            }
            p += RECORD_SIZE;
        }
        return p;
    }

    /**
     * 从指定偏移读取一条命令，不校验记录校验和。
     *
     * @param p 记录起始偏移
     * @return 解码后的命令；命令类型非法时返回 {@code null}
     */
    private Command readCommand(int p) {
        int typeOrdinal = buffer.getInt(p + 4);
        if (typeOrdinal < 0 || typeOrdinal >= COMMAND_TYPES.length) return null;

        long sequence = buffer.getLong(p + 8);
        long orderId = buffer.getLong(p + 16);
        long userId = buffer.getLong(p + 24);
        int symbolId = buffer.getInt(p + 32);
        byte side = buffer.get(p + 36);
        byte orderType = buffer.get(p + 37);
        byte tif = buffer.get(p + 38);
        long price = buffer.getLong(p + 40);
        long qty = buffer.getLong(p + 48);
        long expireAtEpochMillis = buffer.getLong(p + 56);

        return decode(COMMAND_TYPES[typeOrdinal], sequence, orderId, userId, symbolId, side, orderType, tif, price, qty, expireAtEpochMillis);
    }

    /**
     * 判断当前损坏记录是否位于 WAL 尾部。
     *
     * @param position 当前读取偏移
     * @return 后续不存在记录魔数时返回 {@code true}
     */
    private boolean isTailRecord(long position) {
        long next = position + RECORD_SIZE;
        return next + RECORD_SIZE > capacity || buffer.getInt((int) next) != MAGIC;
    }

    /**
     * 将持久化字段解码为命令对象。
     *
     * @param type 命令类型
     * @param sequence 命令序列号
     * @param orderId 订单编号
     * @param userId 用户编号
     * @param symbolId 交易对或分片编号
     * @param side 方向编码
     * @param orderType 订单类型编码
     * @param tif 有效期策略编码
     * @param price 定点数价格
     * @param qty 定点数数量
     * @return 解码后的命令
     */
    private static Command decode(CommandType type, long sequence, long orderId, long userId, int symbolId,
                                  byte side, byte orderType, byte tif, long price, long qty, long expireAtEpochMillis) {
        return switch (type) {
            case NEW_ORDER -> Command.newOrder(sequence, orderId, userId, symbolId,
                    Side.from(side), OrderType.from(orderType), TimeInForce.from(tif), price, qty, expireAtEpochMillis);
            case CANCEL_ORDER -> Command.cancel(sequence, orderId, symbolId);
            case SNAPSHOT_MARKER -> Command.snapshotMarker(sequence, symbolId);
            case SHUTDOWN -> Command.shutdown(sequence);
        };
    }

    /**
     * 为单条命令记录计算轻量确定性校验和。
     *
     * @param c 待计算校验和的命令
     * @return 校验和值
     */
    private static long checksum(Command c) {
        long x = 0x9E3779B97F4A7C15L;
        x ^= c.sequence + (x << 6) + (x >>> 2);
        x ^= c.orderId + (x << 6) + (x >>> 2);
        x ^= c.userId + (x << 6) + (x >>> 2);
        x ^= c.symbolId + (x << 6) + (x >>> 2);
        x ^= c.side + (x << 6) + (x >>> 2);
        x ^= c.orderType + (x << 6) + (x >>> 2);
        x ^= c.timeInForce + (x << 6) + (x >>> 2);
        x ^= c.price + (x << 6) + (x >>> 2);
        x ^= c.quantity + (x << 6) + (x >>> 2);
        x ^= c.expireAtEpochMillis + (x << 6) + (x >>> 2);
        x ^= c.type.ordinal();
        return x;
    }
}
