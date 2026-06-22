package io.github.ike.ullmatcher.ha.coordination;

/**
 * 主租约与 fencing 令牌存储契约。
 * <p>
 * 生产环境应由外部强一致存储实现，例如 etcd、ZooKeeper、Consul 或数据库行锁。
 */
public interface LeaseStore {
    /**
     * 返回当前租约；无租约时返回 {@code null}。
     */
    ClusterLease currentLease();

    /**
     * 在租约缺失、已过期或同 owner 续占的情况下尝试获取租约。
     */
    boolean tryAcquire(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos);

    /**
     * 尝试续租；仅当当前租约 owner 和 fencing token 都匹配时成功。
     */
    boolean tryExtend(String nodeId, FencingToken fencingToken, long nowNanos, long ttlNanos);

    /**
     * 判断当前未过期租约是否由给定 owner 和 fencing token 持有。
     */
    default boolean isHeldBy(String nodeId, FencingToken fencingToken, long nowNanos) {
        ClusterLease lease = currentLease();
        return lease != null
                && !lease.isExpired(nowNanos)
                && lease.ownerNodeId().equals(nodeId)
                && lease.fencingToken().equals(fencingToken);
    }
}
