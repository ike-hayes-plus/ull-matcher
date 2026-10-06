# 复制传输 2.1 架构决议（ADR）

**状态：** 已采纳（2026-10-05）；**2.1 独立发版已取消**，传输统一里程碑并入 **3.0 / 3.0.x**（见 [shard-orchestration-3.0.md](shard-orchestration-3.0.md) §4D）  
**范围：** 单分片节点 HA 复制；不改变 2.0「生产默认 GRPC」口径。

## 背景

当前存在三种 `ReplicationTransportType`：

| 类型 | 用途 |
| --- | --- |
| `GRPC` | 生产默认；长流复制 + mTLS |
| `AERON` | 完整 Aeron 复制（lab / 高级部署） |
| `AERON_PREVIEW` | 简化预览路径；**禁止默认进 PROD** |

实现上 gRPC 与 Aeron 各自维护 codec、peer client 与 metrics，存在重复与 drift 风险。

## 决议

### 1. 生产默认（不变）

- **2.x 生产默认仍为 `GRPC`。**
- 切换至 `AERON` 须：`transport-compare`、soak、回滚演练（见 [ha-sharding-lab.md](../operations/ha-sharding-lab.md)）。

### 2. 「单一实现」的含义（2.1）

指 **语义与框架层统一**，而非删除 GRPC：

- **统一：** 帧/消息边界（`AeronFrameBounds` 与 gRPC proto 适配器对齐）、复制结果与 ack 语义、transport metrics 字段、安全握手状态机。
- **保留：** 可插拔 `ReplicationTransportProvider`（GRPC / AERON 两种生产级后端）。
- **交付顺序：** 2.1 先抽取共享内核与测试矩阵；2.1.x 减少重复 codec 代码。

### 3. `AERON_PREVIEW`

- **2.1 目标：** **废弃并合并** — PREVIEW 行为并入 `AERON` 的「轻量 lab profile」，或在下一大版本 **移除** enum 值。
- **2.1.0 之前：** 配置项仍可用；文档与 CLI 标记 **deprecated**；PROD 闸门保持。
- **删除条件：** lab 基准与 failover smoke 在 GRPC vs AERON（非 PREVIEW）上无回归；无已知外部用户依赖 PREVIEW 专用端口布局。

### 4. 非目标（2.1）

- 3.0 多分片编排；
- 默认 AERON 生产 rollout；
- 跨传输在线热切换（仍须 `allowTransportChange` 窗口）。

## 验证清单（传输统一里程碑）

- [ ] 共享复制语义单测（batch / committed ack）对 GRPC + AERON 双跑
- [ ] `scripts/ops/run-ha-benchmark-suite.sh` floor 不回归
- [x] PREVIEW 在 PROD 闸门内 `@Deprecated`；配置见 [production-deployment-and-capacity.md](../operations/production-deployment-and-capacity.md)

## 关联

- [wal-archive-cold-backup.md](../operations/wal-archive-cold-backup.md) — 2.1 并行交付
- [security-boundary.md](../operations/security-boundary.md)
