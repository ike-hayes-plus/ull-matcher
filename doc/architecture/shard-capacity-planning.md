# 分片容量规划

本文档把当前单分片压测事实源转换成可落地的容量规划建议。

这里采用的是保守口径，目标是规划，不是宣传。

## 术语

- **Accepted throughput**：入口已经受理的订单吞吐
- **Committed throughput**：已经跨过 HA 持久化复制边界的订单吞吐
- **Shard**：一个交易对，或一个独立撮合分区；包含一个 primary 和零个或多个 standby

## 当前单分片事实源

当前基线见：
- [benchmark-baseline-current.md](../operations/benchmark-baseline-current.md)

最重要的规划基线如下：

| 场景 | Accepted orders/s | Committed orders/s |
|---|---:|---:|
| External `1P1S` binary + `GRPC` | `21,648` | `19,935` |
| External `1P1S` binary + `AERON` | `17,360` | `16,137` |
| External `1P2S` binary + `GRPC` quorum | `17,740` | `16,496` |
| External `1P2S` binary + `AERON` quorum | `12,319` | `10,404` |
| External `1P3S` binary + `GRPC` quorum | `13,697` | `12,888` |
| External `1P3S` binary + `AERON` quorum | `27,829` | `21,110` |

## 规划规则

容量规划必须使用 **committed throughput**，不能使用 accepted throughput。

实际规则：

```text
safe shard budget = committed throughput * utilization cap
```

推荐利用率上限：
- `0.60`：生产保守规划
- `0.70`：激进但仍受控的规划

## 示例预算

### `GRPC`，`1P1S`

- committed 基线：`19,935/s`
- `60%` 保守预算：`11,961/s`
- `70%` 激进预算：`13,954/s`

### `GRPC`，`1P2S quorum`

- committed 基线：`16,496/s`
- `60%` 保守预算：`9,897/s`
- `70%` 激进预算：`11,547/s`

### `GRPC`，`1P3S quorum`

- committed 基线：`12,888/s`
- `60%` 保守预算：`7,733/s`
- `70%` 激进预算：`9,021/s`

## 多分片总容量

如果各分片彼此隔离，并且负载分布均衡，总 committed 容量近似为：

```text
total committed capacity ~= shard_count * committed throughput per shard
```

保守规划下：

```text
total safe capacity ~= shard_count * safe shard budget
```

示例：`GRPC 1P1S`，`8` 个分片

- 理论 committed 总量：`8 * 19,935 = 159,480/s`
- `60%` 保守总量：`8 * 11,961 = 95,688/s`

示例：`GRPC 1P2S quorum`，`16` 个分片

- 理论 committed 总量：`16 * 16,496 = 263,936/s`
- `60%` 保守总量：`16 * 9,897 = 158,352/s`

## 这些数字不代表什么

这些数字不代表整个平台一定能稳定跑到同样量级，除非同时满足：

- 分片分配均衡
- CPU 和存储带宽足够
- 入口 fan-out 不产生热点
- 上游账户与风控服务也能同步扩展

## 建议

- 在多备长稳和 chaos 证据不足前，默认传输仍使用 `GRPC`
- 生产分片预算用 **committed throughput** 规划
- accepted throughput 只作为入口余量参考，不作为业务安全吞吐
