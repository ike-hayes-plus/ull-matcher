package io.github.ike.ullmatcher.ha.grpc.server;

import io.github.ike.ullmatcher.ha.grpc.security.GrpcServerTlsConfig;
import io.github.ike.ullmatcher.ha.state.NodeControlStateSource;
import io.github.ike.ullmatcher.ha.snapshot.SnapshotMaterialSource;
import io.github.ike.ullmatcher.ha.standby.StandbySyncService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class GrpcReplicationServer implements Closeable {
    private final Server server;

    public GrpcReplicationServer(GrpcReplicationServerConfig config, StandbySyncService standbySyncService) {
        this(config, new GrpcReplicationService(standbySyncService));
    }

    public GrpcReplicationServer(GrpcReplicationServerConfig config,
                                 StandbySyncService standbySyncService,
                                 NodeControlStateSource nodeControlStateSource,
                                 SnapshotMaterialSource snapshotMaterialSource) {
        this(config, new GrpcReplicationService(standbySyncService, nodeControlStateSource, snapshotMaterialSource));
    }

    public GrpcReplicationServer(GrpcReplicationServerConfig config, GrpcReplicationService service) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(service, "service");
        NettyServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress(config.bindHost(), config.port()))
                .maxInboundMessageSize(config.maxInboundMessageSize())
                .permitKeepAliveTime(config.permitKeepAliveTimeSeconds(), TimeUnit.SECONDS)
                .addService(service);
        if (config.tls() != null) {
            builder.sslContext(serverSslContext(config.tls()));
        }
        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public void shutdown() {
        server.shutdown();
    }

    public int port() {
        return server.getPort();
    }

    @Override
    public void close() {
        shutdown();
    }

    private static io.grpc.netty.shaded.io.netty.handler.ssl.SslContext serverSslContext(GrpcServerTlsConfig tls) {
        try {
            SslContextBuilder builder = GrpcSslContexts.configure(SslContextBuilder.forServer(
                    Files.newInputStream(tls.certificateChainFile()),
                    Files.newInputStream(tls.privateKeyFile())
            ));
            if (tls.trustCertCollectionFile() != null) {
                builder.trustManager(Files.newInputStream(tls.trustCertCollectionFile()));
            }
            if (tls.requireMutualTls()) {
                builder.clientAuth(ClientAuth.REQUIRE);
            }
            return builder.build();
        } catch (IOException e) {
            throw new IllegalStateException("failed to build gRPC server TLS context", e);
        } catch (Exception e) {
            throw new IllegalStateException("failed to initialize gRPC server TLS", e);
        }
    }
}
