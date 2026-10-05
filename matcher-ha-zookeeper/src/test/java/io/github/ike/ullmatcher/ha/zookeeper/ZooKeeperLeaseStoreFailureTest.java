package io.github.ike.ullmatcher.ha.zookeeper;

import io.github.ike.ullmatcher.ha.coordination.ClusterLease;
import io.github.ike.ullmatcher.ha.coordination.FencingToken;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.WatchPathable;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖 ZooKeeperLeaseStore 的注入式 client 生命周期、重入获取与各类 ZooKeeper 故障映射。
 * <p>
 * 需要精确复现的 ZooKeeper 异常用动态代理注入到 Curator 的 builder 上，
 * 避免依赖节点删除与读写之间的时序竞争。
 */
final class ZooKeeperLeaseStoreFailureTest {
    private static final long TTL_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final AtomicInteger PATH_SEQUENCE = new AtomicInteger();

    private static TestingServer server;
    private static CuratorFramework client;

    @BeforeAll
    static void startZooKeeper() throws Exception {
        server = new TestingServer();
        client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(5_000)
                .connectionTimeoutMs(5_000)
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .build();
        client.start();
        assertTrue(client.blockUntilConnected(10, TimeUnit.SECONDS));
    }

    @AfterAll
    static void stopZooKeeper() throws Exception {
        client.close();
        server.close();
    }

    @Test
    void injectedClientSurvivesStoreClose() throws Exception {
        String leasePath = nextLeasePath();
        try (ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(client, leasePath)) {
            assertTrue(store.tryAcquire("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));
        }

        assertEquals(CuratorFrameworkState.STARTED, client.getState(), "store must not own an injected client");
        try (ZooKeeperLeaseStore reader = new ZooKeeperLeaseStore(client, leasePath)) {
            ClusterLease lease = reader.currentLease();
            assertNotNull(lease, "ephemeral node must survive because the session is still alive");
            assertEquals("node-a", lease.ownerNodeId());
            assertEquals(Long.MAX_VALUE, lease.expiresAtNanos());
        }
    }

    @Test
    void currentLeaseReturnsNullWhenTheNodeWasNeverCreated() throws Exception {
        try (ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(client, nextLeasePath())) {
            assertNull(store.currentLease());
        }
    }

    @Test
    void sameOwnerReacquiresItsOwnLeaseWhenTheNodeAlreadyExists() throws Exception {
        String leasePath = nextLeasePath();
        try (ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(client, leasePath)) {
            assertTrue(store.tryAcquire("node-a", new FencingToken(4L), System.nanoTime(), TTL_NANOS));
            assertTrue(store.tryAcquire("node-a", new FencingToken(4L), System.nanoTime(), TTL_NANOS),
                    "re-acquiring an already owned lease must rewrite the payload instead of failing");

            assertFalse(store.tryAcquire("node-a", new FencingToken(5L), System.nanoTime(), TTL_NANOS),
                    "a stale fencing epoch must not win the lease back");

            ClusterLease lease = store.currentLease();
            assertNotNull(lease);
            assertEquals(new FencingToken(4L), lease.fencingToken());
        }
    }

    @Test
    void timingArgumentsAreValidatedForBothAcquireAndExtend() throws Exception {
        try (ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(client, nextLeasePath())) {
            FencingToken token = new FencingToken(1L);

            assertThrows(IllegalArgumentException.class, () -> store.tryAcquire("node-a", token, -1L, TTL_NANOS));
            assertThrows(IllegalArgumentException.class, () -> store.tryAcquire("node-a", token, 1L, 0L));
            assertThrows(IllegalArgumentException.class, () -> store.tryExtend("node-a", token, -1L, TTL_NANOS));
            assertThrows(IllegalArgumentException.class, () -> store.tryExtend("node-a", token, 1L, -1L));
            assertThrows(NullPointerException.class, () -> store.tryAcquire(null, token, 1L, TTL_NANOS));
            assertThrows(NullPointerException.class, () -> store.tryAcquire("node-a", null, 1L, TTL_NANOS));
            assertThrows(NullPointerException.class, () -> store.tryExtend(null, token, 1L, TTL_NANOS));
            assertThrows(NullPointerException.class, () -> store.tryExtend("node-a", null, 1L, TTL_NANOS));
        }
    }

    @Test
    void constructorRejectsMissingCollaborators() {
        assertThrows(NullPointerException.class, () -> new ZooKeeperLeaseStore(null, "/ull/lease"));
        assertThrows(NullPointerException.class, () -> new ZooKeeperLeaseStore(client, null));
    }

    @Test
    void foreignPayloadLayoutIsReportedAsUnreadableLease() throws Exception {
        String leasePath = nextLeasePath();
        client.create().creatingParentsIfNeeded().forPath(leasePath, "not-a-lease".getBytes(StandardCharsets.UTF_8));

        try (ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(client, leasePath)) {
            IllegalStateException error = assertThrows(IllegalStateException.class, store::currentLease);

            assertTrue(error.getMessage().startsWith("failed to read lease from ZooKeeper path"), error.getMessage());
            assertEquals("invalid ZooKeeper lease payload format", error.getCause().getMessage());
        }
    }

    @Test
    void currentLeaseReturnsNullWhenTheNodeVanishesAfterTheExistenceCheck() throws Exception {
        String leasePath = nextLeasePath();
        try (ZooKeeperLeaseStore owner = new ZooKeeperLeaseStore(client, leasePath)) {
            assertTrue(owner.tryAcquire("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));
        }

        ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(
                failingGetData(() -> new KeeperException.NoNodeException(leasePath)), leasePath);

        assertNull(store.currentLease());
    }

    @Test
    void unexpectedReadFailureIsSurfacedAsIllegalState() throws Exception {
        String leasePath = nextLeasePath();
        ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(
                failingMethod("checkExists", () -> new IllegalArgumentException("zookeeper unavailable")), leasePath);

        IllegalStateException error = assertThrows(IllegalStateException.class, store::currentLease);

        assertTrue(error.getMessage().contains(leasePath), error.getMessage());
    }

    @Test
    void unexpectedCreateFailureIsSurfacedAsIllegalState() throws Exception {
        String leasePath = nextLeasePath();
        ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(
                failingMethod("create", () -> new IllegalArgumentException("zookeeper unavailable")), leasePath);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> store.tryAcquire("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));

        assertTrue(error.getMessage().startsWith("failed to acquire lease at ZooKeeper path"), error.getMessage());
    }

    @Test
    void reacquireFailureOnAnExistingNodeIsSurfacedAsIllegalState() throws Exception {
        String leasePath = nextLeasePath();
        try (ZooKeeperLeaseStore owner = new ZooKeeperLeaseStore(client, leasePath)) {
            assertTrue(owner.tryAcquire("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));
        }

        ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(
                failingGetData(() -> new IllegalArgumentException("zookeeper unavailable")), leasePath);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> store.tryAcquire("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));

        assertTrue(error.getMessage().startsWith("failed to reacquire owned lease at ZooKeeper path"), error.getMessage());
    }

    @Test
    void extendReturnsFalseWhenTheLeaseNodeIsGone() throws Exception {
        try (ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(client, nextLeasePath())) {
            assertFalse(store.tryExtend("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));
        }
    }

    @Test
    void extendReturnsFalseOnAVersionConflict() throws Exception {
        String leasePath = nextLeasePath();
        try (ZooKeeperLeaseStore owner = new ZooKeeperLeaseStore(client, leasePath)) {
            assertTrue(owner.tryAcquire("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));
        }

        ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(staleVersionGetData(), leasePath);

        assertFalse(store.tryExtend("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS),
                "a concurrent writer that bumped the znode version must lose the extend");
    }

    @Test
    void unexpectedExtendFailureIsSurfacedAsIllegalState() throws Exception {
        String leasePath = nextLeasePath();
        ZooKeeperLeaseStore store = new ZooKeeperLeaseStore(
                failingGetData(() -> new IllegalArgumentException("zookeeper unavailable")), leasePath);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> store.tryExtend("node-a", new FencingToken(1L), System.nanoTime(), TTL_NANOS));

        assertTrue(error.getMessage().startsWith("failed to extend lease at ZooKeeper path"), error.getMessage());
    }

    private static String nextLeasePath() {
        return "/ull-matcher/failure/shard-" + PATH_SEQUENCE.incrementAndGet() + "/lease";
    }

    /**
     * 让 {@code client.getData()...forPath(...)} 抛出指定异常，其余调用仍然走真实 Curator。
     */
    private static CuratorFramework failingGetData(Supplier<Exception> failure) {
        return proxyClient("getData", () -> getDataBuilder((path, stat) -> {
            throw failure.get();
        }));
    }

    /**
     * 读取真实数据，但把回填的 Stat 版本号改成一个过期值，用来模拟并发写入导致的版本冲突。
     */
    private static CuratorFramework staleVersionGetData() {
        return proxyClient("getData", () -> getDataBuilder((path, stat) -> {
            byte[] data = stat == null
                    ? client.getData().forPath(path)
                    : client.getData().storingStatIn(stat).forPath(path);
            if (stat != null) {
                stat.setVersion(stat.getVersion() + 100);
            }
            return data;
        }));
    }

    private static CuratorFramework failingMethod(String methodName, Supplier<RuntimeException> failure) {
        return proxyClient(methodName, () -> {
            throw failure.get();
        });
    }

    private static CuratorFramework proxyClient(String methodName, Supplier<Object> replacement) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (methodName.equals(method.getName())) {
                return replacement.get();
            }
            try {
                return method.invoke(client, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (CuratorFramework) Proxy.newProxyInstance(
                ZooKeeperLeaseStoreFailureTest.class.getClassLoader(),
                new Class<?>[]{CuratorFramework.class},
                handler
        );
    }

    private static Object getDataBuilder(DataReader reader) {
        AtomicReference<Object> self = new AtomicReference<>();
        AtomicReference<Stat> captured = new AtomicReference<>();
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "storingStatIn" -> {
                captured.set((Stat) args[0]);
                yield self.get();
            }
            case "forPath" -> reader.read((String) args[0], captured.get());
            default -> throw new UnsupportedOperationException(method.getName());
        };
        self.set(Proxy.newProxyInstance(
                ZooKeeperLeaseStoreFailureTest.class.getClassLoader(),
                new Class<?>[]{GetDataBuilder.class, WatchPathable.class},
                handler
        ));
        return self.get();
    }

    @FunctionalInterface
    private interface DataReader {
        byte[] read(String path, Stat stat) throws Exception;
    }
}
