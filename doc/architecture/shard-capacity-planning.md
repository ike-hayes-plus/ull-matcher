# 分片容量规划

本文档把单分片 benchmark 基线转换成可落地的容量规划规则。

这里采用的是保守口径，目标是规划，不是宣传。

## 术语

- **Accepted throughput**：入口已经受理的订单吞吐
- **Trade events throughput**：每秒完成的撮合成交事件数
- **Committed throughput**：已经跨过 HA 持久化复制边界的订单吞吐
- **Shard**：一个交易对，或一个独立撮合分区；包含一个 primary 和零个或多个 standby

## 单分片事实源

基线见：
- [benchmark-baseline.md](../operations/benchmark-baseline.md)

吞吐数字只以 [benchmark-baseline.md](../operations/benchmark-baseline.md) 为准，本文不手抄主表，避免两份文档漂移。

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

- committed 基线：`287,943/s`
- `60%` 保守预算：`172,766/s`
- `70%` 激进预算：`201,560/s`

### `GRPC`，`1P2S quorum`

- committed 基线：`179,250/s`
- `60%` 保守预算：`107,550/s`
- `70%` 激进预算：`125,475/s`

### `GRPC`，`1P3S quorum`

- 本次 quorum committed：`179,508/s`（3 备里确认 2 备即算 committed）
- 本次 `allStandbysDurableCommands=13,760`（小于 32768），不能当作三备都已追上
- 三备都追上之前，不要单独用 quorum committed 做全备 durability 规划

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

- 理论 committed 总量：`8 * 287,943 = 2,303,544/s`
- `60%` 保守总量：`8 * 172,766 = 1,382,128/s`

示例：`GRPC 1P2S quorum`，`16` 个分片

- 理论 committed 总量：`16 * 179,250 = 2,868,000/s`
- `60%` 保守总量：`16 * 107,550 = 1,720,800/s`

## 这些数字不代表什么

这些数字不代表整个平台一定能稳定跑到同样量级，除非同时满足：

- 分片分配均衡
- CPU 和存储带宽足够
- 入口 fan-out 不产生热点
- 上游账户与风控服务也能同步扩展

## 规划原则

- 在多备长稳和 chaos 证据不足前，默认传输仍使用 `GRPC`
- 生产分片预算用 **committed throughput** 规划
- trade events throughput 用来观察真实撮合输出能力
- accepted throughput 只作为入口余量参考，不作为业务安全吞吐
