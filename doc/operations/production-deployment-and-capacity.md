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
- **GRPC** 作为保守默认复制传输
- **AERON** 只在目标拓扑和长稳行为完成验证后启用
- REST HA 表示外部客户端通过 REST 写入 primary，主备复制仍然只走 `GRPC` 或 `AERON`，不走 HTTP

## 2. 稳定事实源

对外口径统一看下面几份文档：

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

## 4. 推荐服务器配置

| 用途 | CPU | 内存 | 磁盘 | 网络 |
| --- | --- | --- | --- | --- |
| REST 普通业务入口 | 8 核以上高主频 CPU | 16 GiB 以上 | NVMe SSD | 10 Gbps |
| Binary 高频入口 | 16 核以上高主频 CPU | 32 GiB 以上 | 低延迟 NVMe SSD | 10 Gbps 以上 |
| 多分片或多备部署 | 按分片独占 CPU 预算 | 64 GiB 以上 | 独立 NVMe 或隔离卷 | 25 Gbps 以上 |

优先保证 CPU 主频、磁盘 fsync 延迟和网络尾延迟。需要更高容量时，优先按 shard 横向扩展。

## 5. 默认部署建议

### 单分片、保守默认

- 传输：`GRPC`
- 拓扑：`1P1S`
- 入口：`binary`

原因：
- 单备 committed 曲线最稳定

### 多备 quorum 拓扑

- 起步优先使用 `GRPC`
- 只有在你明确需要 `AERON` 的拓扑行为，并且愿意自己做 soak / chaos 验证时，再启用 `AERON`

## 6. 脚本边界

- `scripts/deploy/`
  - 正式部署入口
- `scripts/bench/`
  - 压测驱动
- `scripts/lab/`
  - 本地实验拓扑
- `scripts/ops/`
  - 运维采样与 benchmark 报告保存
- `scripts/chaos/`
  - 故障演练与混沌验证

## 7. Benchmark 报告纪律

稳定事实源由基线文档维护，原始 JSON 报告写入 `target/` 下的运行产物目录。

需要保留 benchmark 报告时，使用：

```bash
./scripts/ops/archive-current-benchmarks.sh
```

这样可以保证 README 只引用一套稳定文档口径，避免把运行产物提交到仓库。

## 8. 容量规划规则

容量规划看 committed throughput，不看 accepted throughput。

使用规则：

```text
safe shard budget = committed throughput * utilization cap
```

推荐利用率上限：

- `60%`：保守生产规划
- `70%`：环境高度可控且反复验证后才使用

## 9. 容量不足时的处理顺序

如果单分片 committed 吞吐仍不够，优先顺序应是：

1. 确认是否已经使用 binary ingress
2. 确认复制策略是否符合目标拓扑
3. 先按分片扩容，再考虑继续压单状态机
4. 调整部署建议前，先重跑 committed benchmark

## 10. 稳定性验证

`GRPC` 三节点拓扑建议至少验证：

- 60 秒 soak 采样
- failover smoke
- transport rollout 校验
- cluster readiness 校验

通过标准：

- soak 期间 `allAccepting=true`
- soak 期间 `allServiceReady=true`
- soak 期间 `allClientTrafficReady=true`
- failover smoke 汇总 `success=true`
- transport rollout 校验 `valid=true`

解释方式：

- 这组证据支撑 README 中“`GRPC` 为保守生产默认值”的口径。
- 这组证据不是长时间生产 soak 的替代品。
- `AERON` 作为默认传输前，需要具备同口径的长稳与 chaos 证据。
