# 生产灰度发布 Runbook

本文档描述 `ull-matcher` 的生产灰度发布方式、观察点、回滚步骤，以及 `GRPC` / `AERON` 两种复制传输的建议边界。

配套的逐项勾选清单见：

- [gray-release-checklist.md](gray-release-checklist.md)

## 1. 适用范围

本文档适用于：

- 单交易对 / 单分片撮合集群
- `1 primary + N standbys` 拓扑
- 高频写入口使用 `binary ingress`
- 查询、管理、补单入口使用 `REST`

本文档**不适用于**：

- 同一个 shard 同时存在两个接写 primary
- 同一个 shard 按流量百分比分写到两个不同版本
- 版本间 WAL / snapshot / replication 协议不兼容的迁移场景

## 2. 灰度原则

撮合系统的灰度方式，和普通 Web 服务不同。

普通服务可以按请求百分比分流。  
撮合系统必须优先遵守 **单 shard 单权威主节点** 约束。

因此灰度原则是：

1. **按 shard 灰度，不按订单比例灰度**
2. **先升 standby，再切 primary**
3. **先灰度查询与控制面，再灰度高频写流量**
4. **生产上一次只启用一种复制传输**

## 3. 推荐灰度模型

### 3.1 按 shard 灰度

最推荐。

做法：

- 选择一小部分交易对 / shard 使用新版本
- 其他 shard 继续运行旧版本
- 上游网关按 `symbol` 或 `shardKey` 把流量路由到对应集群

优点：

- 风险隔离清楚
- 不破坏订单顺序
- 回滚边界清晰

### 3.2 单 shard，standby-first 灰度

适合单个 shard 的滚动升级。

做法：

1. 旧版本 primary 持续接流量
2. 新版本节点先以 standby 身份加入
3. standby 完成 catch-up
4. 在窗口内切主到新版本
5. 观察稳定后，再升级旧 primary

优点：

- 不需要双写
- 不拆分写流量
- 复制语义最容易保持一致

### 3.3 入口分层灰度

建议顺序：

1. 先灰度 `REST` 查询与管理流量
2. 再灰度 `binary ingress` 高频写流量

原因：

- 查询和控制面更容易回滚
- 高频写流量对 committed tail 更敏感

## 4. 不建议做的事

### 4.1 不要对同一个 shard 按百分比分流写请求

错误示例：

- 30% 写流量到新版本
- 70% 写流量到旧版本

风险：

- 顺序被拆开
- 订单簿状态分裂
- 撤单和成交结果不可预测

### 4.2 不要在同一个 shard 上做业务双写

除非你做的是严格只读的影子链路，并且新链路不对外返回业务结果，否则不建议做双写。

## 5. 发布前检查

灰度前至少确认：

1. 版本间 WAL、snapshot、replication 协议兼容
2. 新版本已经通过 current 基线对应的 benchmark
3. 已经完成 `soak`、`failover smoke`、`chaos` 基本验证
4. 当前生产拓扑只有一个 `PRIMARY`
5. 监控、日志、证书、数据目录、回滚版本都已准备好

建议先执行：

```bash
./scripts/deploy/gray-release-observe.sh target/gray/before \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081
```

并检查：

- `role`
- `acceptingClientCommands`
- `clientTrafficReady`
- `replicationCommittedSequence`
- `lastDurableSequence`
- `lastAppliedSequence`

## 6. 生产灰度步骤

### 阶段 1：先灰度非交易写流量

先灰度：

- `GET /api/v1/runtime/health`
- `GET /api/v1/runtime/readiness`
- `GET /metrics`
- 查询类 REST
- 管理类接口

目标：

- 先验证配置、控制面、可观测性没有问题

### 阶段 2：standby 先升级

以一个目标 shard 为例：

1. 保持旧版本 primary 在线接流量
2. 拉起新版本 standby
3. 等 standby 完成追平

重点观察：

- `serviceReady=true`
- `clientTrafficReady=false`
- `lastAppliedSequence` 持续追近 primary
- `lastDurableSequence` 持续推进
- `standbyApplyQueueDepth` 不持续堆积
- `standbyAckLastFlushMicros` 不出现异常尖峰

### 阶段 3：切主灰度

在低峰窗口：

1. 下线旧 primary，或按运维窗口触发切主
2. 让新版本 standby 接管为新的 primary
3. 观察稳定窗口

建议在切主前后各采一次集群快照：

```bash
./scripts/deploy/gray-release-observe.sh target/gray/before \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081

# 这里执行人工切主或运维窗口内下线旧 primary

./scripts/deploy/gray-release-observe.sh target/gray/after \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081
```

必须满足：

- 只有一个 `PRIMARY`
- 只有一个 `clientTrafficReady=true`
- 两者是同一节点
- `replicationCommittedSequence` 持续推进
- `replicationQueueDepth` 不异常堆积

### 阶段 4：扩大 shard 范围

推荐顺序：

1. 1 个 shard
2. 5 个 shard
3. 10% shard
4. 30% shard
5. 全量 shard

不要按订单比例放量，必须按 shard 放量。

## 7. 高频入口灰度

### 7.1 binary ingress

推荐：

- 上游网关根据 `symbol` / `shardKey` 路由
- 整个 shard 的 binary 流量切到新版本

不推荐：

- 同 shard 按连接随机切
- 同 shard 按用户随机切
- 同 shard 同时存在两个对外接写 primary

### 7.2 REST

REST 只作为：

- 查询
- 管理
- 补单
- 普通业务接入

可以先用它做控制面和查询面的灰度，不要把它当高频主入口容量验证。

## 8. 观测清单

### 核心接口

- `GET /api/v1/runtime/health`
- `GET /api/v1/runtime/readiness`
- `GET /metrics`

### 核心字段

- `role`
- `acceptingClientCommands`
- `clientTrafficReady`
- `replicationQueueDepth`
- `replicationLastBatchSize`
- `replicationCommittedSequence`
- `replicationLastCommitMicros`
- `lastDurableSequence`
- `lastAppliedSequence`
- `standbyApplyQueueDepth`
- `standbyAckLastFlushMicros`
- `standbyAckLastFlushIntervalMicros`

### 观察重点

- 写流量进入后，`accepted -> committed` 折损是否恶化
- failover 后 committed tail 是否明显拉长
- standby 是否出现持续 apply backlog
- readiness 是否和真实主节点状态一致

## 9. 回滚步骤

### 前提

- 旧版本节点仍可启动
- 旧版本 standby 仍可追平
- 数据目录和协议仍兼容

### 回滚流程

1. 停止扩大新版本 shard 范围
2. 保留旧版本节点并让其追平
3. 将目标 shard 切主回旧版本
4. 观察稳定窗口
5. 确认 `PRIMARY` 与 `clientTrafficReady` 一致
6. 再下线新版本节点

建议回滚后再次执行：

```bash
./scripts/deploy/gray-release-observe.sh target/gray/rollback \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081
```

### 回滚判定

满足以下任一情况，应立即停止继续放量：

- `replicationCommittedSequence` 推进异常变慢
- `standbyApplyQueueDepth` 持续堆积
- `clientTrafficReady` 与真实主节点不一致
- `p99` 或 committed throughput 明显恶化
- 出现 failover 后恢复不完整

## 10. 传输选择建议

### `GRPC`

适合：

- 当前保守生产默认值
- 单备 committed 曲线最稳
- 首次上线和首次灰度

### `AERON`

适合：

- 明确追求特定多备拓扑行为
- 有能力做额外 soak / chaos 验证
- 能接受它不是当前默认主链

当前建议：

- 默认传输保持 `GRPC`
- `AERON` 作为可选高性能传输，在明确验证目标拓扑后启用

## 11. 灰度发布简版清单

```text
1. 先验证 current benchmark 和兼容性
2. 先灰度查询与控制面
3. 新版本先做 standby
4. standby 追平后切主
5. 观察 committed / readiness / backlog
6. 按 shard 扩大范围
7. 出现异常立即按 shard 回滚
```
