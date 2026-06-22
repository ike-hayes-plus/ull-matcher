package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.api.OrderType;
import io.github.ike.ullmatcher.api.Side;
import io.github.ike.ullmatcher.api.TimeInForce;
import io.github.ike.ullmatcher.hft.SubmitResult;
import io.github.ike.ullmatcher.server.engine.MatcherNodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 面向高频接单场景的二进制长连接入口。
 * <p>
 * 该入口保留单节点服务语义，但绕开 HTTP/JSON：
 * <pre>{@code
 * socket -> fixed frame batch -> MatcherNodeService.submit*Batch(...)
 * }</pre>
 * <p>
 * IO 侧使用 selector/event-loop，避免“一连接一线程”的结构性开销。
 */
public final class BinaryOrderIngressServer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BinaryOrderIngressServer.class);
    private static final int REQUEST_MAGIC = 0x554C4C42;
    private static final int RESPONSE_MAGIC = 0x554C4C52;
    private static final short PROTOCOL_VERSION = 1;
    private static final short FRAME_TYPE_NEW_ORDER_BATCH = 1;
    private static final short FRAME_TYPE_CANCEL_ORDER_BATCH = 2;
    private static final short FRAME_TYPE_BATCH_RESULT = 101;
    private static final int FRAME_HEADER_BYTES = 16;
    private static final int REQUEST_RECORD_BYTES = 48;
    private static final int CANCEL_RECORD_BYTES = 16;
    private static final int RESPONSE_RECORD_BYTES = 24;

    private final MatcherNodeService nodeService;
    private final String bindHost;
    private final int requestedPort;
    private final int maxBatchSize;
    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    private final ExecutorService processingExecutor;
    private final ConcurrentLinkedQueue<Runnable> selectorTasks = new ConcurrentLinkedQueue<>();
    private final ArrayDeque<Session> pendingDispatch = new ArrayDeque<>();
    private final Semaphore processingPermits;
    private final AtomicInteger boundPort = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<SocketChannel> openChannels = ConcurrentHashMap.newKeySet();
    private final Thread acceptThread;
    private final int processingParallelism;

    public BinaryOrderIngressServer(String bindHost, int port, int maxBatchSize, MatcherNodeService nodeService) throws IOException {
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost");
        this.requestedPort = port;
        this.maxBatchSize = maxBatchSize;
        this.nodeService = Objects.requireNonNull(nodeService, "nodeService");
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.serverChannel.bind(new InetSocketAddress(bindHost, port));
        this.serverChannel.configureBlocking(false);
        this.processingParallelism = Math.max(2, Math.min(4, Math.max(2, Runtime.getRuntime().availableProcessors() / 3)));
        this.processingExecutor = Executors.newFixedThreadPool(
                processingParallelism,
                Thread.ofPlatform().name("matcher-binary-worker-", 0).factory()
        );
        this.processingPermits = new Semaphore(processingParallelism);
        this.acceptThread = Thread.ofPlatform().name("matcher-binary-accept-" + port).unstarted(this::eventLoop);
    }

    public void start() throws IOException {
        InetSocketAddress address = (InetSocketAddress) serverChannel.getLocalAddress();
        boundPort.set(address.getPort());
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        acceptThread.start();
    }

    public int port() {
        return boundPort.get() == 0 ? requestedPort : boundPort.get();
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        selector.wakeup();
        serverChannel.close();
        try {
            acceptThread.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while stopping binary ingress server", e);
        }
        processingExecutor.shutdownNow();
        try {
            if (!processingExecutor.awaitTermination(5L, TimeUnit.SECONDS)) {
                throw new IOException("timed out while stopping binary ingress workers");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while stopping binary ingress workers", e);
        } finally {
            for (SocketChannel channel : openChannels) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
            }
            selector.close();
        }
    }

    private void eventLoop() {
        try {
            while (!closed.get()) {
                selector.select(250L);
                drainSelectorTasks();
                dispatchQueuedSessions();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    try {
                        if (key.isAcceptable()) {
                            acceptChannel();
                        }
                        if (key.isReadable()) {
                            readSession((Session) key.attachment());
                        }
                        if (key.isWritable()) {
                            writeSession((Session) key.attachment());
                        }
                    } catch (IOException | RuntimeException e) {
                        Session session = (Session) key.attachment();
                        if (session != null) {
                            if (!closed.get() && e.getMessage() != null && !e.getMessage().isBlank()) {
                                LOG.warn("binary ingress session closed due to {}", String.valueOf(e.getMessage()));
                            }
                            closeSession(session);
                        } else if (!closed.get()) {
                            throw e;
                        }
                    }
                }
            }
        } catch (IOException e) {
            if (!closed.get()) {
                throw new RuntimeException("binary ingress accept loop failed", e);
            }
        } finally {
            for (SelectionKey key : selector.keys()) {
                Object attachment = key.attachment();
                if (attachment instanceof Session session) {
                    closeSession(session);
                } else {
                    try {
                        key.channel().close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }

    private void acceptChannel() throws IOException {
        SocketChannel channel = serverChannel.accept();
        if (channel == null) {
            return;
        }
        configureChannel(channel);
        openChannels.add(channel);
        Session session = new Session(channel, maxBatchSize);
        try {
            session.key = channel.register(selector, SelectionKey.OP_READ, session);
        } catch (ClosedChannelException e) {
            openChannels.remove(channel);
            channel.close();
        }
    }

    private void readSession(Session session) throws IOException {
        if (session.processing) {
            return;
        }
        if (session.readingHeader) {
            if (!readInto(session.channel, session.header)) {
                closeSession(session);
                return;
            }
            if (session.header.hasRemaining()) {
                return;
            }
            session.header.flip();
            parseHeader(session);
            session.readingHeader = false;
        }
        if (!readInto(session.channel, session.activeBody)) {
            closeSession(session);
            return;
        }
        if (!session.activeBody.hasRemaining()) {
            session.activeBody.flip();
            dispatchProcessing(session);
        }
    }

    private void writeSession(Session session) throws IOException {
        ByteBuffer response = session.pendingResponse;
        if (response == null) {
            updateInterestOps(session, SelectionKey.OP_READ);
            return;
        }
        session.channel.write(response);
        if (!response.hasRemaining()) {
            session.pendingResponse = null;
            session.resetReadState();
            updateInterestOps(session, SelectionKey.OP_READ);
        }
    }

    private void parseHeader(Session session) throws IOException {
        int magic = session.header.getInt();
        short version = session.header.getShort();
        short frameType = session.header.getShort();
        int recordCount = session.header.getInt();
        int payloadBytes = session.header.getInt();
        session.header.clear();

        if (magic != REQUEST_MAGIC || version != PROTOCOL_VERSION) {
            throw new IOException("unsupported binary ingress frame");
        }
        if (recordCount < 0 || recordCount > maxBatchSize) {
            throw new IOException("binary ingress record count exceeds max batch size");
        }
        session.frameType = frameType;
        session.recordCount = recordCount;
        if (frameType == FRAME_TYPE_NEW_ORDER_BATCH) {
            if (payloadBytes != recordCount * REQUEST_RECORD_BYTES) {
                throw new IOException("binary ingress payload size mismatch");
            }
            session.activeBody = session.requestBody;
        } else if (frameType == FRAME_TYPE_CANCEL_ORDER_BATCH) {
            if (payloadBytes != recordCount * CANCEL_RECORD_BYTES) {
                throw new IOException("binary ingress cancel payload size mismatch");
            }
            session.activeBody = session.cancelBody;
        } else {
            throw new IOException("unsupported binary ingress frame");
        }
        session.activeBody.clear();
        session.activeBody.limit(payloadBytes);
    }

    private void dispatchProcessing(Session session) throws IOException {
        updateInterestOps(session, 0);
        if (!processingPermits.tryAcquire()) {
            if (!session.dispatchQueued) {
                session.dispatchQueued = true;
                pendingDispatch.addLast(session);
            }
            return;
        }
        session.processing = true;
        try {
            processingExecutor.execute(() -> processSessionFrame(session));
        } catch (RejectedExecutionException e) {
            session.processing = false;
            processingPermits.release();
            throw new IOException("binary ingress workers are saturated", e);
        }
    }

    private void processSessionFrame(Session session) {
        try {
            ByteBuffer response;
            if (session.frameType == FRAME_TYPE_NEW_ORDER_BATCH) {
                for (int i = 0; i < session.recordCount; i++) {
                    session.userIds[i] = session.requestBody.getLong();
                    session.orderIds[i] = session.requestBody.getLong();
                    session.prices[i] = session.requestBody.getLong();
                    session.quantities[i] = session.requestBody.getLong();
                    session.ttlMillis[i] = session.requestBody.getLong();
                    session.sides[i] = decodeSide(session.requestBody.get());
                    session.orderTypes[i] = decodeOrderType(session.requestBody.get());
                    session.timeInForces[i] = decodeTimeInForce(session.requestBody.get());
                    session.requestBody.position(session.requestBody.position() + 5);
                }
                response = buildNewOrderResponse(session);
            } else if (session.frameType == FRAME_TYPE_CANCEL_ORDER_BATCH) {
                for (int i = 0; i < session.recordCount; i++) {
                    session.orderIds[i] = session.cancelBody.getLong();
                    session.cancelBody.getLong();
                }
                response = buildCancelResponse(session);
            } else {
                throw new IOException("unsupported binary ingress frame");
            }
            selectorTasks.add(() -> {
                if (closed.get()) {
                    processingPermits.release();
                    closeSession(session);
                    return;
                }
                processingPermits.release();
                session.processing = false;
                session.pendingResponse = response;
                try {
                    updateInterestOps(session, SelectionKey.OP_WRITE);
                } catch (IOException e) {
                    closeSession(session);
                }
            });
            selector.wakeup();
        } catch (IOException | RuntimeException e) {
            if (!closed.get() && e.getMessage() != null && !e.getMessage().isBlank()) {
                LOG.warn("binary ingress session closed due to {}", String.valueOf(e.getMessage()));
            }
            selectorTasks.add(() -> {
                processingPermits.release();
                session.processing = false;
                closeSession(session);
            });
            selector.wakeup();
        }
    }

    private void updateInterestOps(Session session, int interestOps) throws IOException {
        SelectionKey key = session.key;
        if (key == null || !key.isValid()) {
            throw new IOException("binary ingress session is not registered");
        }
        key.interestOps(interestOps);
    }

    private void closeSession(Session session) {
        try {
            if (session.key != null) {
                session.key.cancel();
            }
        } catch (RuntimeException ignored) {
        }
        openChannels.remove(session.channel);
        try {
            session.channel.close();
        } catch (IOException ignored) {
        }
    }

    private void drainSelectorTasks() {
        Runnable task;
        while ((task = selectorTasks.poll()) != null) {
            task.run();
        }
    }

    private void dispatchQueuedSessions() {
        while (!pendingDispatch.isEmpty() && processingPermits.availablePermits() > 0) {
            Session session = pendingDispatch.pollFirst();
            if (session == null) {
                break;
            }
            session.dispatchQueued = false;
            if (session.key == null || !session.key.isValid() || session.pendingResponse != null || session.processing || session.readingHeader) {
                continue;
            }
            try {
                dispatchProcessing(session);
            } catch (IOException e) {
                closeSession(session);
            }
        }
    }

    private ByteBuffer buildNewOrderResponse(Session session) throws IOException {
        ByteBuffer body = session.responseBody;
        body.clear();
        nodeService.submitNewOrderBatch(session.newOrderSource, (index, result, sequence) -> {
            body.putLong(session.orderIds[index]);
            body.putLong(sequence);
            body.putInt(statusCode(result));
            body.putInt(0);
        });
        return wrapResponseFrame(session, body, session.recordCount);
    }

    private ByteBuffer buildCancelResponse(Session session) throws IOException {
        ByteBuffer body = session.responseBody;
        body.clear();
        nodeService.submitCancelOrderBatch(session.cancelOrderSource, (index, result, sequence) -> {
            body.putLong(session.orderIds[index]);
            body.putLong(sequence);
            body.putInt(statusCode(result));
            body.putInt(0);
        });
        return wrapResponseFrame(session, body, session.recordCount);
    }

    private static ByteBuffer wrapResponseFrame(Session session, ByteBuffer body, int count) {
        body.flip();
        ByteBuffer frame = session.responseFrame;
        frame.clear();
        frame.putInt(RESPONSE_MAGIC);
        frame.putShort(PROTOCOL_VERSION);
        frame.putShort(FRAME_TYPE_BATCH_RESULT);
        frame.putInt(count);
        frame.putInt(body.remaining());
        frame.put(body);
        frame.flip();
        return frame;
    }

    private static boolean readInto(SocketChannel channel, ByteBuffer buffer) throws IOException {
        int read = channel.read(buffer);
        return read >= 0;
    }

    private static void configureChannel(SocketChannel channel) throws IOException {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
    }

    private static Side decodeSide(byte encoded) {
        return switch (encoded) {
            case 'B' -> Side.BUY;
            case 'S' -> Side.SELL;
            default -> throw new IllegalArgumentException("unsupported side: " + encoded);
        };
    }

    private static OrderType decodeOrderType(byte encoded) {
        return switch (encoded) {
            case 'L' -> OrderType.LIMIT;
            case 'M' -> OrderType.MARKET_WITH_PROTECTION;
            default -> throw new IllegalArgumentException("unsupported order type: " + encoded);
        };
    }

    private static TimeInForce decodeTimeInForce(byte encoded) {
        return switch (encoded) {
            case 'G' -> TimeInForce.GTC;
            case 'I' -> TimeInForce.IOC;
            case 'F' -> TimeInForce.FOK;
            default -> throw new IllegalArgumentException("unsupported timeInForce: " + encoded);
        };
    }

    private static int statusCode(SubmitResult result) {
        return switch (result) {
            case ACCEPTED -> 0;
            case MATCHER_NOT_RUNNING -> 1;
            case RING_FULL_BEFORE_WAL_APPEND -> 2;
            case COMMAND_POOL_EXHAUSTED -> 3;
            case MATCHER_STOPPED_AFTER_WAL_APPEND -> 4;
            case RING_FULL_AFTER_WAL_APPEND -> 5;
        };
    }

    private static final class Session {
        private final SocketChannel channel;
        private final ByteBuffer header = ByteBuffer.allocateDirect(FRAME_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        private final ByteBuffer requestBody;
        private final ByteBuffer cancelBody;
        private final ByteBuffer responseBody;
        private final ByteBuffer responseFrame;
        private final long[] userIds;
        private final long[] orderIds;
        private final long[] prices;
        private final long[] quantities;
        private final long[] ttlMillis;
        private final Side[] sides;
        private final OrderType[] orderTypes;
        private final TimeInForce[] timeInForces;
        private final MatcherNodeService.NewOrderBatchSource newOrderSource;
        private final MatcherNodeService.CancelOrderBatchSource cancelOrderSource;
        private SelectionKey key;
        private ByteBuffer activeBody;
        private ByteBuffer pendingResponse;
        private boolean readingHeader = true;
        private boolean processing;
        private boolean dispatchQueued;
        private short frameType;
        private int recordCount;

        private Session(SocketChannel channel, int maxBatchSize) {
            this.channel = channel;
            this.requestBody = ByteBuffer.allocateDirect(maxBatchSize * REQUEST_RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
            this.cancelBody = ByteBuffer.allocateDirect(maxBatchSize * CANCEL_RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
            this.responseBody = ByteBuffer.allocateDirect(maxBatchSize * RESPONSE_RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
            this.responseFrame = ByteBuffer.allocateDirect(FRAME_HEADER_BYTES + (maxBatchSize * RESPONSE_RECORD_BYTES)).order(ByteOrder.BIG_ENDIAN);
            this.userIds = new long[maxBatchSize];
            this.orderIds = new long[maxBatchSize];
            this.prices = new long[maxBatchSize];
            this.quantities = new long[maxBatchSize];
            this.ttlMillis = new long[maxBatchSize];
            this.sides = new Side[maxBatchSize];
            this.orderTypes = new OrderType[maxBatchSize];
            this.timeInForces = new TimeInForce[maxBatchSize];
            this.newOrderSource = new MatcherNodeService.NewOrderBatchSource() {
                @Override
                public int size() {
                    return recordCount;
                }

                @Override
                public long userIdAt(int index) {
                    return userIds[index];
                }

                @Override
                public long orderIdAt(int index) {
                    return orderIds[index];
                }

                @Override
                public Side sideAt(int index) {
                    return sides[index];
                }

                @Override
                public OrderType orderTypeAt(int index) {
                    return orderTypes[index];
                }

                @Override
                public TimeInForce timeInForceAt(int index) {
                    return timeInForces[index];
                }

                @Override
                public long priceAt(int index) {
                    return prices[index];
                }

                @Override
                public long quantityAt(int index) {
                    return quantities[index];
                }

                @Override
                public Long ttlMillisAt(int index) {
                    return ttlMillis[index] < 0L ? null : ttlMillis[index];
                }
            };
            this.cancelOrderSource = new MatcherNodeService.CancelOrderBatchSource() {
                @Override
                public int size() {
                    return recordCount;
                }

                @Override
                public long orderIdAt(int index) {
                    return orderIds[index];
                }
            };
        }

        private void resetReadState() {
            readingHeader = true;
            processing = false;
            dispatchQueued = false;
            frameType = 0;
            recordCount = 0;
            activeBody = null;
            header.clear();
            requestBody.clear();
            cancelBody.clear();
        }
    }
}
