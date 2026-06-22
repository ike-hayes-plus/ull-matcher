# v0.1.0 发布说明草稿

本文档是 `ull-matcher` 首版开源发布草稿，基于当前仓库状态、当前稳定 benchmark 口径和当前默认值建议整理。

---

## 1. 版本信息

- 版本号：`v0.1.0`
- 发布时间：待定
- 发布类型：首版开源发布
- 适用范围：
  - 开源用户
  - 本地实验与容量验证
  - 生产 PoC / 灰度验证

## 2. 本次发布摘要

本次版本完成了单分片撮合内核、高频 binary ingress、WAL、主备复制、灰度发布文档、容量规划文档和 current 稳定基线的第一轮收口，当前保守生产默认复制传输为 `GRPC`。

## 3. 主要变更

### 3.1 新增能力

- 提供单线程撮合核心与独立服务节点两种使用形态
- 提供通用 REST 接口与高频 binary ingress 两套入口
- 提供 WAL、快照、重放、主备复制与自动接管能力
- 提供灰度发布 runbook、检查清单、发布说明模板和容量规划文档

### 3.2 性能与吞吐改进

- 核心撮合主链补齐了 core-only benchmark，单 JVM 主链进入百万级 ops/s 与微秒级延迟量级
- 高频主入口明确切到 binary ingress，单节点 binary 场景稳定到 `~20k orders/s`
- `ReplicationCoordinator`、`MatcherNodeService`、binary ingress 和 standby ack/flush 路径已做多轮减分配和批处理收敛

### 3.3 高可用与稳定性改进

- committed watermark 进入一等指标面
- binary replication-commit benchmark 已覆盖 `1P1S / 1P2S / 1P3S`
- 灰度发布观测脚本与严格集群校验已补齐
- `GRPC` 三节点拓扑已补过最终 soak / failover 收尾证据

### 3.4 文档与脚本改进

- `README` 已收成开源首页口径
- `deploy / bench / lab / ops / chaos` 脚本边界已分层
- `deploy` 脚本不再只是 `lab` 的别名
- 对外文档不再依赖 `target/current/**` 作为 GitHub 可见产物路径

## 4. 不兼容变更

- 无

## 5. 默认值与推荐口径

当前推荐：

- 高频主入口：`binary ingress`
- 通用入口：`REST`
- 默认复制传输：`GRPC`
- 可选高性能传输：`AERON`

说明：

- 默认值表示当前保守生产建议，不表示所有拓扑都绝对最快。
- `AERON` 当前可作为高性能可选实现，但是否替代默认值，仍建议先补目标拓扑的 soak / chaos 证据。

## 6. 基准与容量口径

本次发布中的 benchmark 需要按口径理解：

- `Core-only matcher`
  - 不包含 network
  - 不包含 IPC
  - 不包含 WAL
  - 不包含 HA
- `Embedded journaled core`
  - 包含本地 WAL
  - 不包含外部进程边界
  - 不包含 HA
- `External ... replication committed`
  - 包含 binary ingress
  - 包含外部进程边界
  - 包含 HA 持久化复制闭环

生产容量规划应以 **replication committed throughput** 为准。

参考：

- [benchmark-baseline-current.md](benchmark-baseline-current.md)
- [production-deployment-and-capacity.md](production-deployment-and-capacity.md)
- [shard-capacity-planning.md](../architecture/shard-capacity-planning.md)

## 7. 当前稳定事实源

### 核心与单节点

- Core-only matcher
  - accepted orders/s：`6,992,019.06`
  - p99：`0.46 µs`
- Embedded journaled core
  - accepted orders/s：`48,977.35`
- Single-node binary
  - accepted orders/s：`20,075.79`
  - p99：`3.74 ms`

### 外部 `binary + HA committed`

- External `1P1S` binary + `GRPC`
  - accepted：`21,648.10`
  - committed：`19,935.02`
  - catch-up：`0.0081 s`
  - p99：`2.48 ms`
- External `1P1S` binary + `AERON`
  - accepted：`17,359.70`
  - committed：`16,136.80`
  - catch-up：`0.0089 s`
  - p99：`3.13 ms`
- External `1P2S` binary + `GRPC` quorum
  - accepted：`17,739.98`
  - committed：`16,495.97`
  - catch-up：`0.0087 s`
  - p99：`3.06 ms`
- External `1P2S` binary + `AERON` quorum
  - accepted：`12,319.45`
  - committed：`10,404.45`
  - catch-up：`0.0306 s`
  - p99：`4.61 ms`
- External `1P3S` binary + `GRPC` quorum
  - accepted：`13,697.46`
  - committed：`12,887.54`
  - catch-up：`0.0094 s`
  - p99：`4.17 ms`
- External `1P3S` binary + `AERON` quorum
  - accepted：`27,829.30`
  - committed：`21,109.76`
  - catch-up：`0.0234 s`
  - p99：`2.19 ms`

## 8. 灰度与发布建议

推荐方式：

- 按 shard 灰度
- 先升 standby，再切 primary
- 先灰度查询与控制面，再灰度高频写流量
- 对同一个 shard，不要做按百分比分写

参考：

- [gray-release-runbook.md](gray-release-runbook.md)
- [gray-release-checklist.md](gray-release-checklist.md)

## 9. 回滚条件

建议至少满足以下任一条件即进入回滚评估：

- `replicationCommittedSequence` 推进明显变慢
- `standbyApplyQueueDepth` 持续堆积
- `clientTrafficReady` 与真实主节点不一致
- `p99` 或 committed throughput 明显恶化
- failover 后恢复不完整

## 10. 发布前检查清单

- [ ] 全量测试已通过
- [ ] current benchmark 口径已核对
- [ ] 默认值说明已核对
- [ ] 灰度 runbook 已核对
- [ ] 回滚方案已准备
- [ ] 文档索引已更新

## 11. 已知限制

- 当前默认值是保守生产建议，不代表所有拓扑下绝对最快
- `AERON` 仍建议在目标拓扑下单独完成 soak / chaos 验证
- 单分片吞吐不等于整个平台总吞吐
- 当前单机单分片继续优化已进入小收益阶段，进一步扩容更应优先依赖分片

## 12. 相关文档

- [README.md](../../README.md)
- [production-deployment-and-capacity.md](production-deployment-and-capacity.md)
- [benchmark-baseline-current.md](benchmark-baseline-current.md)
- [gray-release-runbook.md](gray-release-runbook.md)
- [gray-release-checklist.md](gray-release-checklist.md)
- [release-note-template.md](release-note-template.md)
- [shard-capacity-planning.md](../architecture/shard-capacity-planning.md)
