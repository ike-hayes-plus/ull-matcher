# 复制传输架构（ADR）

**状态：** 已采纳  
**范围：** 单分片节点 HA 复制。

## 背景

| 类型 | 用途 |
| --- | --- |
| `GRPC` | **生产默认**；长流复制 + mTLS |
| `AERON` | 完整 Aeron 复制（lab / 高级部署） |
| `AERON_PREVIEW` | lab 预览路径；PROD 默认禁止 |

gRPC 与 Aeron 各自维护 codec、peer client 与 metrics；长期方向是 **语义与 metrics 统一**，保留可插拔 `ReplicationTransportProvider`。

## 决议

### 生产默认

- 生产默认 **`GRPC`**。
- 切换至 `AERON` 须：`transport-compare`、soak、回滚演练（见 [ha-sharding-lab.md](../operations/ha-sharding-lab.md)）。

### 语义统一（后续里程碑）

- 统一帧/消息边界、复制 ack 语义、transport metrics、安全握手状态机。
- 不删除 GRPC / AERON 双后端。

### `AERON_PREVIEW`

- 枚举已 `@Deprecated(forRemoval = true)`；PROD 须显式 `matcher.allowPreviewTransportInProd=true`。
- 合并进 `AERON` lab profile 或移除 enum 的前置条件：lab failover / 基准无回归。

### 非目标

- 多分片编排（见 [shard-orchestration.md](shard-orchestration.md)）；
- 默认 AERON 生产 rollout；
- 无窗口的在线传输热切换（仍须 `allowTransportChange` + `transportChangeWindowId`）。

## 验证清单

- [ ] 共享复制语义单测对 GRPC + AERON 双跑
- [x] HA 报告 floor：`scripts/ops/check-ha-benchmark-floor.sh`（suite 只产出 JSON）
- [x] PREVIEW PROD 闸门与文档

## 关联

- [wal-archive-cold-backup.md](../operations/wal-archive-cold-backup.md)
- [security-boundary.md](../operations/security-boundary.md)
