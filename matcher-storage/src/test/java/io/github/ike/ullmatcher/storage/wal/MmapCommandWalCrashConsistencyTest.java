package io.github.ike.ullmatcher.storage.wal;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash consistency for the mmap WAL.
 * <p>
 * The writer is run in a forked JVM that is killed with {@code Runtime.halt} so no shutdown hook,
 * {@code close()} or finalizer ever runs. Everything the recovering reader sees therefore came from
 * {@link MmapCommandWal#force()} alone, which is exactly the guarantee the commit path depends on.
 */
final class MmapCommandWalCrashConsistencyTest {
    private static final int SYMBOL = 1001;
    private static final long FILE_SIZE = MmapCommandWal.RECORD_SIZE * 64L;

    @Test
    void forcedRecordsSurviveAProcessThatNeverClosesTheWal(@TempDir Path directory) throws Exception {
        Path wal = directory.resolve("crash.wal");

        int exitCode = runCrashingWriter(wal, 10);

        assertEquals(CrashingWalWriter.HALT_CODE, exitCode, "writer must die without closing the WAL");
        assertEquals(sequences(1L, 10L), replay(wal));
    }

    @Test
    void recoveryResumesAppendingAfterTheLastForcedRecord(@TempDir Path directory) throws Exception {
        Path wal = directory.resolve("resume.wal");
        runCrashingWriter(wal, 5);

        try (MmapCommandWal reopened = new MmapCommandWal(wal, FILE_SIZE)) {
            assertEquals(5 * MmapCommandWal.RECORD_SIZE, reopened.writePosition());
            reopened.append(newSell(6L));
            reopened.force();
        }

        assertEquals(sequences(1L, 6L), replay(wal));
    }

    @Test
    void aTornTrailingRecordIsDroppedInsteadOfFailingRecovery(@TempDir Path directory) throws Exception {
        Path wal = directory.resolve("torn.wal");
        runCrashingWriter(wal, 4);

        // Simulate a record whose payload reached the device but whose checksum did not.
        corruptByte(wal, (4 * MmapCommandWal.RECORD_SIZE) - 1);

        assertEquals(sequences(1L, 3L), replay(wal));
    }

    @Test
    void recoveryStopsAtTheFirstUnwrittenRecord(@TempDir Path directory) throws Exception {
        Path wal = directory.resolve("short.wal");
        runCrashingWriter(wal, 3);

        try (MmapCommandWal reopened = new MmapCommandWal(wal, FILE_SIZE)) {
            reopened.resetReader();
            for (long expected = 1L; expected <= 3L; expected++) {
                Command command = reopened.next();
                assertNotNull(command, "record " + expected + " must be readable");
                assertEquals(expected, command.sequence);
            }
            assertNull(reopened.next(), "reader must stop at the first unwritten slot");
        }
    }

    private static List<Long> replay(Path wal) throws IOException {
        List<Long> sequences = new ArrayList<>();
        try (MmapCommandWal reopened = new MmapCommandWal(wal, FILE_SIZE)) {
            reopened.resetReader();
            Command command;
            while ((command = reopened.next()) != null) {
                sequences.add(command.sequence);
            }
        }
        return sequences;
    }

    private static List<Long> sequences(long fromInclusive, long toInclusive) {
        List<Long> expected = new ArrayList<>();
        for (long sequence = fromInclusive; sequence <= toInclusive; sequence++) {
            expected.add(sequence);
        }
        return expected;
    }

    private static void corruptByte(Path wal, long offset) throws IOException {
        try (FileChannel channel = FileChannel.open(wal, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer original = ByteBuffer.allocate(1);
            channel.read(original, offset);
            byte flipped = (byte) (original.get(0) ^ 0xFF);
            channel.write(ByteBuffer.wrap(new byte[]{flipped}), offset);
            channel.force(true);
        }
    }

    private static int runCrashingWriter(Path wal, int recordCount) throws IOException, InterruptedException {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        ProcessBuilder builder = new ProcessBuilder(
                java.toString(),
                "-cp", System.getProperty("java.class.path"),
                CrashingWalWriter.class.getName(),
                wal.toString(),
                Integer.toString(recordCount)
        ).redirectErrorStream(true);
        Process process = builder.start();
        final String output;
        try (InputStream input = process.getInputStream()) {
            output = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertTrue(process.waitFor(60L, TimeUnit.SECONDS), "writer process timed out: " + output);
        return process.exitValue();
    }

    static Command newSell(long sequence) {
        return Command.newOrder(sequence, sequence, 10 + sequence, SYMBOL, Side.SELL,
                OrderType.LIMIT, TimeInForce.GTC, 100 + sequence, 1);
    }

    /**
     * Writer entry point for the forked JVM. Appends, forces, then halts without unwinding.
     */
    public static final class CrashingWalWriter {
        static final int HALT_CODE = 87;

        private CrashingWalWriter() {}

        public static void main(String[] args) throws Exception {
            Path wal = Path.of(args[0]);
            int recordCount = Integer.parseInt(args[1]);
            Files.createDirectories(wal.toAbsolutePath().getParent());
            MmapCommandWal commandWal = new MmapCommandWal(wal, FILE_SIZE);
            for (long sequence = 1; sequence <= recordCount; sequence++) {
                commandWal.append(newSell(sequence));
                commandWal.force();
            }
            Runtime.getRuntime().halt(HALT_CODE);
        }
    }
}
