package io.github.ike.ullmatcher.server.security;

import io.github.ike.ullmatcher.ha.transport.TransportSecuritySnapshot;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * 维护节点间传输证书材料的当前视图，并在文件变更后原地重载。
 * 该上下文同时向复制传输层暴露代际、重载次数和失败状态，用于 readiness 与混沌校验。
 */
public final class ReloadableTransportSecurityContext implements Closeable {
    private final TransportSecurityConfig config;
    private final long reloadIntervalMillis;
    private final AtomicReference<TransportSecurityMaterials> current = new AtomicReference<>();
    private final AtomicLong generation = new AtomicLong(1L);
    private final AtomicLong reloadCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();
    private final WatchService watchService;
    private final Set<Path> watchedFiles;
    private final Thread reloadThread;

    private volatile boolean running = true;
    private volatile FileTime certChainMtime;
    private volatile FileTime privateKeyMtime;
    private volatile FileTime trustChainMtime;
    private volatile boolean reloading;
    private volatile String lastError = "";

    public ReloadableTransportSecurityContext(ServerSecurityConfig securityConfig) throws IOException, GeneralSecurityException {
        Objects.requireNonNull(securityConfig, "securityConfig");
        if (securityConfig.transportSecurity() == null) {
            throw new IllegalArgumentException("transportSecurity is required");
        }
        this.config = securityConfig.transportSecurity();
        this.reloadIntervalMillis = securityConfig.tlsReloadIntervalMillis();
        this.current.set(TransportSecurityLoader.load(config, generation.get()));
        initializeMtlsState();
        this.watchService = securityConfig.tlsReloadEnabled() ? FileSystems.getDefault().newWatchService() : null;
        this.watchedFiles = securityConfig.tlsReloadEnabled() ? watchedFiles(config) : Set.of();
        if (watchService != null) {
            registerWatchDirectories();
        }
        this.reloadThread = securityConfig.tlsReloadEnabled()
                ? Thread.ofPlatform().name("transport-security-reload").start(this::reloadLoop)
                : null;
    }

    public TransportSecurityMaterials currentMaterials() {
        return current.get();
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

    @Override
    public void close() throws IOException {
        running = false;
        if (reloadThread != null) {
            reloadThread.interrupt();
            try {
                reloadThread.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping transport security reload thread", e);
            }
        }
        if (watchService != null) {
            watchService.close();
        }
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
                    try {
                        reloading = true;
                        Thread.sleep(Math.max(100L, reloadIntervalMillis));
                        if (materialsChanged()) {
                            current.set(TransportSecurityLoader.load(config, generation.incrementAndGet()));
                            initializeMtlsState();
                            reloadCount.incrementAndGet();
                            lastError = "";
                        }
                    } catch (IOException | GeneralSecurityException | RuntimeException e) {
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

    private boolean materialsChanged() throws IOException {
        return changed(config.certificateChainFile(), certChainMtime)
                || changed(config.privateKeyFile(), privateKeyMtime)
                || (config.trustCertCollectionFile() != null && changed(config.trustCertCollectionFile(), trustChainMtime));
    }

    private void initializeMtlsState() throws IOException {
        certChainMtime = Files.getLastModifiedTime(config.certificateChainFile());
        privateKeyMtime = Files.getLastModifiedTime(config.privateKeyFile());
        trustChainMtime = config.trustCertCollectionFile() == null
                ? null
                : Files.getLastModifiedTime(config.trustCertCollectionFile());
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

    private static boolean changed(Path file, FileTime previous) throws IOException {
        return previous == null || !Files.getLastModifiedTime(file).equals(previous);
    }

    private static Set<Path> watchedFiles(TransportSecurityConfig config) {
        Set<Path> files = new HashSet<>();
        files.add(config.certificateChainFile().toAbsolutePath().normalize());
        files.add(config.privateKeyFile().toAbsolutePath().normalize());
        if (config.trustCertCollectionFile() != null) {
            files.add(config.trustCertCollectionFile().toAbsolutePath().normalize());
        }
        return Set.copyOf(files);
    }
}
