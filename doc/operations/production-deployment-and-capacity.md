# 生产部署与容量总览

本文档把部署形态、压测事实源和分片容量规划收成一份生产总览。

在阅读项目 README 之后，建议先看这份文档。

## 1. 推荐生产姿态

- 高频订单流量优先使用 **binary ingress**
- **REST** 保留给：
  - 管理面
  - 订单 / 状态查询
  - 后台与普通业务接入
  - 低频策略流量
- **GRPC** 作为当前保守默认复制传输
- **AERON** 只在你已经准备好验证目标拓扑和长稳行为时再启用

## 2. 当前稳定事实源

当前对外口径统一看下面几份文档：

- [benchmark-baseline-current.md](benchmark-baseline-current.md)
- [gray-release-runbook.md](gray-release-runbook.md)
- [shard-capacity-planning.md](../architecture/shard-capacity-planning.md)
- [deployment-modes.md](deployment-modes.md)

## 3. 如何理解吞吐

需要区分三类数字：

1. **纯撮合主链**
   - 只测撮合核心
   - 不包含网络、IPC、WAL 和 HA

2. **Accepted throughput**
   - 入口已经受理命令
   - 适合判断入口余量

3. **Replication committed throughput**
   - 命令已经跨过 HA 持久化边界
   - 这是生产容量规划应该使用的数字

生产规划必须以 **replication committed throughput** 为准。

## 4. 默认部署建议

### 单分片、保守默认

- 传输：`GRPC`
- 拓扑：`1P1S`
- 入口：`binary`

原因：
- 当前单备 committed 曲线最稳定

### 多备 quorum 拓扑

- 起步优先使用 `GRPC`
- 只有在你明确需要 `AERON` 的拓扑行为，并且愿意自己做 soak / chaos 验证时，再启用 `AERON`

## 5. 脚本边界

- `scripts/deploy/`
  - 正式部署入口
- `scripts/bench/`
  - 压测驱动
- `scripts/lab/`
  - 本地实验拓扑
- `scripts/ops/`
  - 运维采样与基线归档
- `scripts/chaos/`
  - 故障演练与混沌验证

## 6. 归档纪律

当前稳定事实源由基线文档维护，原始 JSON 报告只在本地工作区归档。

历史快照统一归档到：

```bash
./scripts/ops/archive-current-benchmarks.sh
```

这样可以保证 README 只引用一套 current 文档口径，同时保留本地历史归档用于回归比较。

## 7. 容量规划规则

容量规划看 committed throughput，不看 accepted throughput。

使用规则：

```text
safe shard budget = committed throughput * utilization cap
```

推荐利用率上限：

- `60%`：保守生产规划
- `70%`：环境高度可控且反复验证后才使用

## 8. 后续优化优先级

如果单分片 committed 吞吐仍不够，优先顺序应是：

1. 确认是否已经使用 binary ingress
2. 确认复制策略是否符合目标拓扑
3. 先按分片扩容，再考虑继续压单状态机
4. 调整生产建议前，先重跑 committed benchmark

## 9. 发布前稳定性证据

当前发布前稳定性证据基于一轮 `GRPC` 三节点拓扑收尾验证：

- 60 秒 soak 采样
- failover smoke
- transport rollout 校验
- cluster readiness 校验

核心结论：

- soak 期间 `allAccepting=true`
- soak 期间 `allServiceReady=true`
- soak 期间 `allClientTrafficReady=true`
- failover smoke 汇总 `success=true`
- transport rollout 校验 `valid=true`

建议解释方式：

- 这组证据足以支撑当前 README 中“`GRPC` 为保守生产默认值”的口径。
- 这组证据仍然不是长时间生产 soak 的替代品。
- 如果要把 `AERON` 提升为默认值，必须先补同口径的长稳与 chaos 证据。
