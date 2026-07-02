package io.github.ike.ullmatcher.server.security;

import io.github.ike.ullmatcher.ha.transport.TransportSecuritySnapshot;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServer;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationServerConfig;
import io.github.ike.ullmatcher.ha.grpc.server.GrpcReplicationService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileTime;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

public final class ReloadableGrpcServer implements Closeable {
    private final Supplier<GrpcReplicationServerConfig> configSupplier;
    private final Supplier<GrpcReplicationService> serviceSupplier;
    private final ServerSecurityConfig securityConfig;
    private final AtomicReference<GrpcReplicationServer> delegate = new AtomicReference<>();
    private final Thread reloadThread;
    private final WatchService watchService;
    private final Set<Path> watchedFiles;
    private final AtomicLong generation = new AtomicLong(1L);
    private final AtomicLong reloadCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();

    private volatile boolean running = true;
    private volatile FileTime certChainMtime;
    private volatile FileTime privateKeyMtime;
    private volatile FileTime trustChainMtime;
    private volatile boolean reloading;
    private volatile String lastError = "";

    public ReloadableGrpcServer(Supplier<GrpcReplicationServerConfig> configSupplier,
                                Supplier<GrpcReplicationService> serviceSupplier,
                                ServerSecurityConfig securityConfig) throws IOException {
        this.configSupplier = Objects.requireNonNull(configSupplier, "configSupplier");
        this.serviceSupplier = Objects.requireNonNull(serviceSupplier, "serviceSupplier");
        this.securityConfig = Objects.requireNonNull(securityConfig, "securityConfig");
        GrpcReplicationServer server = newServer();
        delegate.set(server);
        initializeMtlsState();
        this.watchService = securityConfig.tlsReloadEnabled() ? java.nio.file.FileSystems.getDefault().newWatchService() : null;
        this.watchedFiles = securityConfig.tlsReloadEnabled() ? watchedFiles(securityConfig) : Set.of();
        if (watchService != null) {
            registerWatchDirectories();
        }
        this.reloadThread = securityConfig.tlsReloadEnabled()
                ? Thread.ofPlatform().name("grpc-tls-reload").start(this::reloadLoop)
                : null;
    }

    public void start() throws IOException {
        delegate.get().start();
    }

    public int port() {
        return delegate.get().port();
    }

    private void reloadLoop() {
        while (running) {
            try {
                WatchKey key = watchService.take();
                boolean relevantChange = false;
                Path directory = (Path) key.watchable();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path changed = directory.resolve((Path) event.context()).toAbsolutePath().normalize();
                    if (watchedFiles.contains(changed)) {
                        relevantChange = true;
                    }
                }
                key.reset();
                if (relevantChange) {
                    // Stage a replacement server before the cutover, then swap after a short debounce window.
                    try {
                        reloading = true;
                        GrpcReplicationServer replacement = newServer();
                        Thread.sleep(Math.max(100L, securityConfig.tlsReloadIntervalMillis()));
                        if (tlsMaterialsChanged()) {
                            reload(replacement);
                            reloadCount.incrementAndGet();
                            generation.incrementAndGet();
                            lastError = "";
                        } else {
                            replacement.close();
                        }
                    } catch (IOException | RuntimeException e) {
                        failureCount.incrementAndGet();
                        lastError = e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage());
                    } finally {
                        reloading = false;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private synchronized void reload(GrpcReplicationServer replacement) throws IOException {
        GrpcReplicationServer previous = delegate.get();
        previous.close();
        replacement.start();
        delegate.set(replacement);
        initializeMtlsState();
    }

    private GrpcReplicationServer newServer() {
        return new GrpcReplicationServer(configSupplier.get(), serviceSupplier.get());
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (reloadThread != null) {
            reloadThread.interrupt();
            try {
                reloadThread.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping TLS reload thread", e);
            }
        }
        if (watchService != null) {
            watchService.close();
        }
        delegate.get().close();
    }

    private void initializeMtlsState() throws IOException {
        if (securityConfig.grpcServerTls() == null) {
            return;
        }
        certChainMtime = readMtime(securityConfig.grpcServerTls().certificateChainFile());
        privateKeyMtime = readMtime(securityConfig.grpcServerTls().privateKeyFile());
        trustChainMtime = securityConfig.grpcServerTls().trustCertCollectionFile() == null
                ? null
                : readMtime(securityConfig.grpcServerTls().trustCertCollectionFile());
    }

    private boolean tlsMaterialsChanged() throws IOException {
        if (securityConfig.grpcServerTls() == null) {
            return false;
        }
        return changed(securityConfig.grpcServerTls().certificateChainFile(), certChainMtime)
                || changed(securityConfig.grpcServerTls().privateKeyFile(), privateKeyMtime)
                || (securityConfig.grpcServerTls().trustCertCollectionFile() != null
                && changed(securityConfig.grpcServerTls().trustCertCollectionFile(), trustChainMtime));
    }

    private static boolean changed(Path file, FileTime previous) throws IOException {
        return previous == null || !readMtime(file).equals(previous);
    }

    private static FileTime readMtime(Path file) throws IOException {
        return Files.getLastModifiedTime(file);
    }

    private static Set<Path> watchedFiles(ServerSecurityConfig securityConfig) {
        Set<Path> files = new HashSet<>();
        files.add(securityConfig.grpcServerTls().certificateChainFile().toAbsolutePath().normalize());
        files.add(securityConfig.grpcServerTls().privateKeyFile().toAbsolutePath().normalize());
        if (securityConfig.grpcServerTls().trustCertCollectionFile() != null) {
            files.add(securityConfig.grpcServerTls().trustCertCollectionFile().toAbsolutePath().normalize());
        }
        return files;
    }

    private void registerWatchDirectories() throws IOException {
        Set<Path> directories = new HashSet<>();
        for (Path file : watchedFiles) {
            Path directory = file.getParent();
            if (directory != null && directories.add(directory)) {
                directory.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
            }
        }
    }

    public TransportSecuritySnapshot snapshot() {
        return new TransportSecuritySnapshot(
                generation.get(),
                reloadCount.get(),
                failureCount.get(),
                reloading,
                lastError
        );
    }
}
