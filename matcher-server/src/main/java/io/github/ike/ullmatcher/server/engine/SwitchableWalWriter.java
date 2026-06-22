package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.api.Command;
import io.github.ike.ullmatcher.storage.wal.WalWriter;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

final class SwitchableWalWriter implements WalWriter {
    private final WalWriter localWal;
    private final AtomicReference<WalWriter> delegate;

    SwitchableWalWriter(WalWriter localWal) {
        this.localWal = Objects.requireNonNull(localWal, "localWal");
        this.delegate = new AtomicReference<>(localWal);
    }

    void delegateTo(WalWriter walWriter) {
        delegate.set(Objects.requireNonNull(walWriter, "walWriter"));
    }

    void resetToLocal() {
        delegate.set(localWal);
    }

    @Override
    public void append(Command command) throws IOException {
        delegate.get().append(command);
    }

    @Override
    public void force() throws IOException {
        delegate.get().force();
    }

    @Override
    public void close() {
    }
}
