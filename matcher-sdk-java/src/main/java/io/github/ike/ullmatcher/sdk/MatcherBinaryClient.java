package io.github.ike.ullmatcher.sdk;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * matcher binary ingress 的阻塞式客户端。
 * <p>
 * 该客户端面向低延迟批量接单路径，直接按服务端固定帧协议写入 TCP 连接。
 * 一个实例持有一条长连接；调用方应按连接粒度串行使用，或为不同发送线程创建独立实例。
 */
public final class MatcherBinaryClient implements Closeable {
    /** 请求帧 magic: ASCII "ULLB"。 */
    private static final int REQUEST_MAGIC = 0x554C4C42;
    /** 响应帧 magic: ASCII "ULLR"。 */
    private static final int RESPONSE_MAGIC = 0x554C4C52;
    private static final short PROTOCOL_VERSION = 1;
    private static final short FRAME_TYPE_NEW_ORDER_BATCH = 1;
    private static final short FRAME_TYPE_CANCEL_ORDER_BATCH = 2;
    private static final short FRAME_TYPE_BATCH_RESULT = 101;
    /** 固定帧头：magic/version/type/count/payloadBytes。 */
    private static final int FRAME_HEADER_BYTES = 16;
    /** 新单记录固定 48 字节，与 BinaryOrderIngressServer 保持一致。 */
    private static final int REQUEST_RECORD_BYTES = 48;
    /** 撤单记录固定 16 字节：orderId + reserved。 */
    private static final int CANCEL_RECORD_BYTES = 16;
    /** 响应记录固定 24 字节：orderId + sequence + resultCode + reserved。 */
    private static final int RESPONSE_RECORD_BYTES = 24;

    private final Socket socket;
    private final Duration timeout;

    public MatcherBinaryClient(String host, int port, Duration timeout) throws IOException {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(timeout, "timeout");
        this.timeout = timeout;
        this.socket = new Socket();
        int timeoutMillis = Math.toIntExact(Math.min(Integer.MAX_VALUE, timeout.toMillis()));
        // binary ingress 是长连接协议；连接超时和读超时都使用调用方提供的请求预算。
        socket.connect(new InetSocketAddress(host, port), timeoutMillis);
        socket.setSoTimeout(timeoutMillis);
        // 接单路径不依赖 Nagle 合并，默认关闭以降低单批次尾延迟。
        socket.setTcpNoDelay(true);
    }

    /**
     * 批量提交新单。
     *
     * @param orders 新单记录，按列表顺序编码为一个 batch frame
     * @return 服务端逐条返回的本地提交结果
     * @throws IOException 网络或协议错误
     */
    public List<BinaryCommandResult> submitOrders(List<BinaryNewOrder> orders) throws IOException {
        if (orders == null || orders.isEmpty()) {
            throw new IllegalArgumentException("orders must not be empty");
        }
        ByteBuffer payload = ByteBuffer.allocate(orders.size() * REQUEST_RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
        for (BinaryNewOrder order : orders) {
            // 字段顺序必须与服务端 BinaryOrderIngressServer 的解码顺序一致。
            payload.putLong(order.userId());
            payload.putLong(order.orderId());
            payload.putLong(order.price());
            payload.putLong(order.quantity());
            payload.putLong(order.ttlMillis());
            payload.put(order.side());
            payload.put(order.orderType());
            payload.put(order.timeInForce());
            payload.put(new byte[5]);
        }
        return sendFrame(FRAME_TYPE_NEW_ORDER_BATCH, orders.size(), payload);
    }

    /**
     * 批量撤单。
     *
     * @param orderIds 需要撤销的订单编号
     * @return 服务端逐条返回的本地提交结果
     * @throws IOException 网络或协议错误
     */
    public List<BinaryCommandResult> cancelOrders(List<Long> orderIds) throws IOException {
        if (orderIds == null || orderIds.isEmpty()) {
            throw new IllegalArgumentException("orderIds must not be empty");
        }
        ByteBuffer payload = ByteBuffer.allocate(orderIds.size() * CANCEL_RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
        for (Long orderId : orderIds) {
            if (orderId == null || orderId <= 0L) {
                throw new IllegalArgumentException("orderId must be positive");
            }
            payload.putLong(orderId);
            payload.putLong(0L);
        }
        return sendFrame(FRAME_TYPE_CANCEL_ORDER_BATCH, orderIds.size(), payload);
    }

    private List<BinaryCommandResult> sendFrame(short frameType, int recordCount, ByteBuffer payload) throws IOException {
        payload.flip();
        ByteBuffer frame = ByteBuffer.allocate(FRAME_HEADER_BYTES + payload.remaining()).order(ByteOrder.BIG_ENDIAN);
        // 统一的大端固定帧头便于跨语言 SDK 复刻，也便于抓包排查。
        frame.putInt(REQUEST_MAGIC);
        frame.putShort(PROTOCOL_VERSION);
        frame.putShort(frameType);
        frame.putInt(recordCount);
        frame.putInt(payload.remaining());
        frame.put(payload);
        socket.getOutputStream().write(frame.array());
        socket.getOutputStream().flush();
        return readResponse();
    }

    private List<BinaryCommandResult> readResponse() throws IOException {
        byte[] headerBytes = socket.getInputStream().readNBytes(FRAME_HEADER_BYTES);
        if (headerBytes.length != FRAME_HEADER_BYTES) {
            throw new IOException("binary ingress response header truncated");
        }
        ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
        int magic = header.getInt();
        short version = header.getShort();
        short frameType = header.getShort();
        int count = header.getInt();
        int payloadBytes = header.getInt();
        // 响应头先做强校验，避免在协议错位时把任意字节解释为结果。
        if (magic != RESPONSE_MAGIC || version != PROTOCOL_VERSION || frameType != FRAME_TYPE_BATCH_RESULT) {
            throw new IOException("invalid binary ingress response header");
        }
        if (payloadBytes != count * RESPONSE_RECORD_BYTES) {
            throw new IOException("invalid binary ingress response payload size");
        }
        byte[] payload = socket.getInputStream().readNBytes(payloadBytes);
        if (payload.length != payloadBytes) {
            throw new IOException("binary ingress response payload truncated");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        ArrayList<BinaryCommandResult> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            results.add(new BinaryCommandResult(buffer.getLong(), buffer.getLong(), buffer.getInt(), buffer.getInt()));
        }
        return results;
    }

    public Duration timeout() {
        return timeout;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
