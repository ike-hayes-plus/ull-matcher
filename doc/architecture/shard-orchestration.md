# 多分片编排（ADR）

**状态：** 已采纳  
**基线：** 见 [README.md](../../README.md)。

## 1. 目标

把多个 **单 symbol / 单线程 / 单 shardKey** 的 `matcher-server` 进程编排成可运维整体：

| 能力 | 说明 |
| --- | --- |
| 分片注册 | 控制面可见的 shard 生命周期（active / draining / absent） |
| Symbol 路由 | 上游或边车根据 `symbolId` → `shardKey` → 节点端点 |
| 快照协议一致 | 各分片沿用 snapshot + WAL manifest；编排层只聚合元数据 |
| 分片级扩缩容 | 新增 shard、drain、摘流；**不**在线改单进程内 symbol 列表 |

## 2. 非目标

- 单 JVM / 单 match loop 内 **多 symbol 共享订单簿**
- 跨 symbol **原子撮合**
- 默认切换生产复制传输为 AERON（复制仍见 [replication-transport.md](replication-transport.md)）
- 替代上游 API 网关的全部路由职责

## 3. 架构草图

```text
          ┌─────────────────────────────────────┐
          │  编排面（matcher-orchestrator）        │
          │  路由表 · shard 注册 · drain API       │
          └──────────────┬──────────────────────┘
                         │ etcd
     ┌───────────────────┼───────────────────┐
     ▼                   ▼                   ▼
 matcher-server      matcher-server       matcher-server
 shardKey=A           shardKey=B           shardKey=C
```

每个分片进程内部：单 ring、单 WAL、单 HA 复制组。

## 4. 交付状态

- [x] `matcher-orchestrator` + `EtcdOrchestratorStore`
- [x] `matcher.orchestratorEnabled`（独立 JVM）/ `ull.matcher.orchestrator-enabled`（Spring）自注册 / heartbeat / drain；须 etcd 控制面
- [x] HTTP `GET /api/v1/orchestrator/routes/symbols/{symbolId}` + SDK
- [x] `RoutingTable.startBackgroundRefresh`（嵌入方周期刷新）
- [ ] etcd watch 推送（可选增强）
- [ ] 三 shard lab + 多 shard runbook

## 5. 控制面键空间

```text
{etcdKeyPrefix}/v3/routes/symbols/{symbolId}  → { shardKey, generation, updatedAt }
{etcdKeyPrefix}/v3/shards/{shardKey}          → { nodeId, http, grpc, binary, role, state }
```

`etcdKeyPrefix` 与现有控制面 prefix 相同：独立 JVM 默认 `/ull-matcher/{clusterName}`，Spring 默认 `/ull-matcher/{cluster.name}`。例如 cluster `prod` 的 symbol 7 路由键是 `/ull-matcher/prod/v3/routes/symbols/7`。

未开启 orchestrator 时，单节点部署行为与无编排时相同。

## 6. 关联文档

- [shard-model-design.md](shard-model-design.md)
- [shard-capacity-planning.md](shard-capacity-planning.md)
- [replication-transport.md](replication-transport.md)
