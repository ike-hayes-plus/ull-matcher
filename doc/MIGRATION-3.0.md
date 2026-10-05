# 迁移到 3.0（多分片编排）

**状态：** 编写中（3.0 开发分支）  
**适用对象：** 已运行 2.0 **单分片节点**、需要 **多 symbol 运维面统一** 的团队。

## 与 2.0 的关系

- **2.0 节点二进制与协议保持兼容**；未部署编排层时，行为与 2.0 相同。
- **3.0 不引入** 单进程多 symbol 撮合；每个 symbol 仍对应独立 `matcher-server`（或等价进程）与独立 `shardKey`。
- **2.1 独立版本线已取消**；WAL 冷备等已随 `master` 提供，见 [wal-archive-cold-backup.md](operations/wal-archive-cold-backup.md)。

## 3.0 新增组件（计划）

| 组件 | 作用 |
| --- | --- |
| `matcher-orchestrator` | 路由表、shard 注册、drain/scale 运维 API |
| SDK 可选模式 | 通过 orchestrator 解析 `symbolId` → 节点端点 |

详细架构见 [architecture/shard-orchestration-3.0.md](architecture/shard-orchestration-3.0.md)。

## 配置变更（草案）

| 2.x | 3.0 |
| --- | --- |
| `matcher.clusterName` | **移除**；使用 `matcher.cluster` |
| 上游自行维护 symbol→host | 可选编排自注册 + 后续 SDK 解析 |
| （无） | `-Dmatcher.orchestratorEnabled=true` + 现有 `matcher.etcdEndpoint`（须 etcd 控制面） |
| （无） | `-Dmatcher.orchestratorGeneration=1`（路由 generation，默认 1） |

## 迁移步骤（GA 时填充）

1. 部署 orchestrator（或与现有 etcd 同集群）。
2. 为每个 symbol 注册 `shardKey` 与端点（与现有 HA 注册并存）。
3. 上游网关或 SDK 切到 orchestrator 路由（金丝雀 → 全量）。
4. 验证 drain：新 shard 上线、旧 shard 只读/摘流。

## 回滚

- 关闭 orchestrator 路由，恢复 **2.0 静态路由**（网关配置原 primary 列表）。
- 单分片进程无需降级二进制。
