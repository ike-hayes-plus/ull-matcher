package io.github.ike.ullmatcher.storage.wal;

import io.github.ike.ullmatcher.api.Command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * 基于 {@link MmapCommandWal} 的分段 WAL。
 * <p>
 * 单个分段写满后自动滚动到新文件，恢复时按分段起始序列号顺序读取。快照成功后，
 * 可删除已经被快照覆盖的旧分段，并通过 {@link WalSegmentArchiver} 接入远端归档。
 * <p>
 * 文件命名格式：{@code <prefix>-00000000000000000001.wal}，数字部分是该分段第一条命令的序列号。
 */
public final class SegmentedMmapWal implements WalWriter, WalReader {
    /** WAL 文件扩展名。 */
    private static final String EXTENSION = ".wal";

    /** 文件名中序列号的固定宽度。 */
    private static final int SEQUENCE_WIDTH = 20;

    /** 分段所在目录。 */
    private final Path directory;

    /** 分段文件名前缀。 */
    private final String filePrefix;

    /** 单个分段文件大小，单位为字节。 */
    private final long segmentSizeBytes;

    /** 旧分段删除前的归档器。 */
    private final WalSegmentArchiver archiver;

    /** 当前写入分段。 */
    private MmapCommandWal writer;

    /** 当前写入分段路径。 */
    private Path writerPath;

    /** 当前读取分段。 */
    private MmapCommandWal reader;

    /** 当前读取分段列表。 */
    private List<Path> readerSegments = List.of();

    /** 下一次要打开的读取分段下标。 */
    private int readerIndex;

    /**
     * 创建不执行远端归档的分段 WAL。
     *
     * @param directory 分段所在目录
     * @param filePrefix 分段文件名前缀
     * @param segmentSizeBytes 单个分段文件大小，单位为字节
     * @throws IOException 分段目录或现有分段无法打开时抛出
     */
    public SegmentedMmapWal(Path directory, String filePrefix, long segmentSizeBytes) throws IOException {
        this(directory, filePrefix, segmentSizeBytes, WalSegmentArchiver.noop());
    }

    /**
     * 创建分段 WAL。
     *
     * @param directory 分段所在目录
     * @param filePrefix 分段文件名前缀
     * @param segmentSizeBytes 单个分段文件大小，单位为字节
     * @param archiver 旧分段删除前的归档器
     * @throws IOException 分段目录或现有分段无法打开时抛出
     */
    public SegmentedMmapWal(Path directory, String filePrefix, long segmentSizeBytes,
                            WalSegmentArchiver archiver) throws IOException {
        if (segmentSizeBytes < MmapCommandWal.RECORD_SIZE) {
            throw new IllegalArgumentException("segmentSizeBytes must be at least one WAL record");
        }
        this.directory = directory;
        this.filePrefix = filePrefix;
        this.segmentSizeBytes = segmentSizeBytes;
        this.archiver = archiver;
        Files.createDirectories(directory);
        openLastSegmentForAppend();
        resetReader();
    }

    /**
     * 追加命令，当前分段写满时自动滚动到新分段。
     *
     * @param command 待持久化命令
     * @throws IOException 分段滚动或写入失败时抛出
     */
    @Override
    public void append(Command command) throws IOException {
        if (writer == null) {
            openWriter(segmentPath(command.sequence));
        }
        if (writer.writePosition() + MmapCommandWal.RECORD_SIZE > segmentSizeBytes) {
            rollTo(command.sequence);
        }
        writer.append(command);
    }

    @Override
    public void appendAll(Iterable<Command> commands) throws IOException {
        if (commands instanceof List<Command> list) {
            appendList(list);
            return;
        }
        for (Command command : commands) {
            append(command);
        }
    }

    /**
     * 强制刷新当前写入分段。
     *
     * @throws IOException 刷盘失败时抛出
     */
    @Override
    public void force() throws IOException {
        if (writer != null) writer.force();
    }

    private void appendList(List<Command> commands) throws IOException {
        if (commands.isEmpty()) {
            return;
        }
        int offset = 0;
        while (offset < commands.size()) {
            Command first = commands.get(offset);
            if (writer == null) {
                openWriter(segmentPath(first.sequence));
            }
            int availableRecords = (int) ((segmentSizeBytes - writer.writePosition()) / MmapCommandWal.RECORD_SIZE);
            if (availableRecords <= 0) {
                rollTo(first.sequence);
                availableRecords = (int) ((segmentSizeBytes - writer.writePosition()) / MmapCommandWal.RECORD_SIZE);
            }
            int chunkSize = Math.min(commands.size() - offset, availableRecords);
            writer.appendAll(commands.subList(offset, offset + chunkSize));
            offset += chunkSize;
        }
    }

    /**
     * 顺序读取下一条命令，读完当前分段后自动切到下一个分段。
     *
     * @return 下一条命令；所有分段读完时返回 {@code null}
     * @throws IOException 分段读取失败时抛出
     */
    @Override
    public Command next() throws IOException {
        while (true) {
            if (reader == null && !openNextReader()) return null;
            Command command = reader.next();
            if (command != null) return command;
            reader.close();
            reader = null;
            readerIndex++;
        }
    }

    /**
     * 重置读取游标，并重新扫描当前目录下的 WAL 分段。
     *
     * @throws IOException 分段目录无法读取时抛出
     */
    public void resetReader() throws IOException {
        closeReader();
        readerSegments = listSegments();
        readerIndex = 0;
    }

    /**
     * 删除已经被快照覆盖的旧 WAL 分段。
     * <p>
     * 当前正在写入的分段不会被删除，即使它已被快照覆盖；下一次滚动后会参与清理。
     *
     * @param snapshotSequence 快照包含的最后命令序列号
     * @return 本次清理结果
     * @throws IOException 归档、扫描或删除失败时抛出
     */
    public WalRetentionResult deleteSegmentsCoveredBySnapshot(long snapshotSequence) throws IOException {
        List<Path> archived = new ArrayList<>();
        List<Path> deleted = new ArrayList<>();
        for (Path segment : listSegments()) {
            if (segment.equals(writerPath)) continue;
            long lastSequence = lastSequence(segment);
            if (lastSequence > 0 && lastSequence <= snapshotSequence) {
                archiver.archive(segment);
                archived.add(segment);
                Files.deleteIfExists(segment);
                deleted.add(segment);
            }
        }
        if (!deleted.isEmpty()) {
            StorageSync.forceDirectory(directory);
        }
        resetReader();
        return new WalRetentionResult(snapshotSequence, archived, deleted);
    }

    /**
     * 为当前快照和 WAL 分段写入检查点清单。
     *
     * @param snapshotPath 快照文件路径
     * @param snapshotSequence 快照覆盖的最后命令序列号
     * @param snapshotTradeId 快照覆盖的最后成交编号
     * @return 写入后的清单
     * @throws IOException 当前 WAL 无法刷盘或清单无法写入时抛出
     */
    public WalManifest writeManifest(Path snapshotPath, long snapshotSequence, long snapshotTradeId) throws IOException {
        force();
        return WalManifest.write(manifestPath(), snapshotPath, snapshotSequence, snapshotTradeId, listSegments());
    }

    /**
     * 读取当前 WAL 命名空间的检查点清单。
     *
     * @return 清单对象
     * @throws IOException 清单无法读取时抛出
     */
    public WalManifest readManifest() throws IOException {
        return WalManifest.read(manifestPath());
    }

    /**
     * 返回当前 WAL 命名空间的检查点清单路径。
     *
     * @return 清单路径
     */
    public Path manifestPath() {
        return directory.resolve(filePrefix + ".manifest");
    }

    /**
     * 返回当前写入分段路径。
     *
     * @return 当前写入分段路径；尚未写入任何命令时返回 {@code null}
     */
    public Path currentSegmentPath() {
        return writerPath;
    }

    /**
     * 返回当前写入分段已写入字节数。
     *
     * @return 当前写入分段字节数；尚未打开写分段时返回 0
     */
    public long currentSegmentBytes() {
        return writer == null ? 0L : writer.writePosition();
    }

    /**
     * 返回当前目录下按起始序列号排序的 WAL 分段。
     *
     * @return WAL 分段路径列表
     * @throws IOException 分段目录无法读取时抛出
     */
    public List<Path> segments() throws IOException {
        return listSegments();
    }

    /**
     * 关闭当前读写分段。
     *
     * @throws IOException 关闭失败时抛出
     */
    @Override
    public void close() throws IOException {
        IOException error = null;
        try {
            closeReader();
        } catch (IOException e) {
            error = e;
        }
        try {
            if (writer != null) writer.close();
        } catch (IOException e) {
            if (error == null) error = e;
            else error.addSuppressed(e);
        } finally {
            writer = null;
            writerPath = null;
        }
        if (error != null) throw error;
    }

    /**
     * 打开最后一个现有分段用于追加。
     *
     * @throws IOException 分段无法打开时抛出
     */
    private void openLastSegmentForAppend() throws IOException {
        List<Path> segments = listSegments();
        if (!segments.isEmpty()) {
            openWriter(segments.get(segments.size() - 1));
        }
    }

    /**
     * 滚动到以指定序列号开头的新分段。
     *
     * @param firstSequence 新分段第一条命令序列号
     * @throws IOException 分段无法关闭或打开时抛出
     */
    private void rollTo(long firstSequence) throws IOException {
        if (writer != null) {
            writer.force();
            writer.close();
        }
        openWriter(segmentPath(firstSequence));
    }

    /**
     * 打开指定路径作为当前写入分段。
     *
     * @param path 分段路径
     * @throws IOException 分段无法打开时抛出
     */
    private void openWriter(Path path) throws IOException {
        writer = new MmapCommandWal(path, segmentSizeBytes);
        writerPath = path;
        StorageSync.forceDirectory(directory);
    }

    /**
     * 打开下一个读取分段。
     *
     * @return 成功打开时返回 {@code true}；没有更多分段时返回 {@code false}
     * @throws IOException 分段无法打开时抛出
     */
    private boolean openNextReader() throws IOException {
        if (readerIndex >= readerSegments.size()) return false;
        reader = new MmapCommandWal(readerSegments.get(readerIndex), segmentSizeBytes);
        reader.resetReader();
        return true;
    }

    /**
     * 关闭当前读取分段。
     *
     * @throws IOException 分段关闭失败时抛出
     */
    private void closeReader() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    /**
     * 返回指定分段的最后一条命令序列号。
     *
     * @param segment 分段路径
     * @return 最后一条命令序列号；空分段返回 {@code 0}
     * @throws IOException 分段无法读取时抛出
     */
    private long lastSequence(Path segment) throws IOException {
        long last = 0;
        try (MmapCommandWal wal = new MmapCommandWal(segment, segmentSizeBytes)) {
            Command command;
            while ((command = wal.next()) != null) {
                last = command.sequence;
            }
        }
        return last;
    }

    /**
     * 扫描并排序当前目录下的 WAL 分段。
     *
     * @return WAL 分段路径列表
     * @throws IOException 分段目录无法读取时抛出
     */
    private List<Path> listSegments() throws IOException {
        if (!Files.exists(directory)) return List.of();
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                    .filter(this::isSegment)
                    .sorted(Comparator.comparingLong(this::firstSequence))
                    .toList();
        }
    }

    /**
     * 判断路径是否属于当前 WAL 分段命名空间。
     *
     * @param path 待检查路径
     * @return 属于当前 WAL 分段时返回 {@code true}
     */
    private boolean isSegment(Path path) {
        String name = path.getFileName().toString();
        if (!name.startsWith(filePrefix + "-") || !name.endsWith(EXTENSION)) return false;
        String seq = sequencePart(name);
        if (seq.length() != SEQUENCE_WIDTH) return false;
        for (int i = 0; i < seq.length(); i++) {
            if (!Character.isDigit(seq.charAt(i))) return false;
        }
        return true;
    }

    /**
     * 生成指定起始序列号对应的分段路径。
     *
     * @param firstSequence 分段第一条命令序列号
     * @return 分段路径
     */
    private Path segmentPath(long firstSequence) {
        if (firstSequence <= 0) {
            throw new IllegalArgumentException("firstSequence must be positive");
        }
        return directory.resolve(filePrefix + "-" +
                String.format(Locale.ROOT, "%0" + SEQUENCE_WIDTH + "d", firstSequence) + EXTENSION);
    }

    /**
     * 解析分段文件名中的起始序列号。
     *
     * @param path 分段路径
     * @return 起始序列号
     */
    private long firstSequence(Path path) {
        return Long.parseLong(sequencePart(path.getFileName().toString()));
    }

    /**
     * 返回文件名中的序列号片段。
     *
     * @param name 文件名
     * @return 序列号字符串
     */
    private String sequencePart(String name) {
        return name.substring(filePrefix.length() + 1, name.length() - EXTENSION.length());
    }
}
