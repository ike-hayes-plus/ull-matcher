# 3.0 多分片编排（ADR）

**状态：** 已采纳（2026-10-05）  
**前置：** 2.0 单分片节点已可生产部署（见 [cto-signoff-2.0.md](../operations/cto-signoff-2.0.md)）  
**版本策略：** **跳过 2.1 独立发版线**；已合并的 2.1 能力（WAL 冷备、传输 ADR）计入 3.0 基线；传输 codec 统一与 `AERON_PREVIEW` 移除并入 3.0 里程碑或 3.0.x 补丁。

## 1. 目标

把多个 **单 symbol / 单线程 / 单 shardKey** 的 `matcher-server` 进程编排成可运维整体：

| 能力 | 说明 |
| --- | --- |
| 分片注册 | 控制面可见的 shard 生命周期（active / draining / absent） |
| Symbol 路由 | 上游或边车根据 `symbolId` → `shardKey` → 节点端点 |
| 快照协议一致 | 各分片沿用现有 snapshot + WAL manifest；编排层只聚合元数据 |
| 分片级扩缩容 | 新增 shard 进程、drain、摘流；**不**在线改单进程内 symbol 列表 |

## 2. 非目标（3.0 明确不做）

- 单 JVM / 单 match loop 内 **多 symbol 共享订单簿状态机**
- 跨 symbol **原子撮合**
- 默认切换生产复制传输为 AERON（仍 GRPC 默认，与 2.0 会审一致）
- 替代上游 API 网关的全部路由职责（3.0 提供 **权威路由表与运维 API**，业务网关可复用）

## 3. 架构草图

```text
          ┌─────────────────────────────────────┐
          │  3.0 编排面（新模块，见 §4）           │
          │  路由表 · shard 注册 · drain/scale API │
          └──────────────┬──────────────────────┘
                         │ 读/写 etcd 或 ZK（复用现有 registry 前缀扩展）
     ┌───────────────────┼───────────────────┐
     ▼                   ▼                   ▼
 matcher-server      matcher-server       matcher-server
 shardKey=A           shardKey=B           shardKey=C
 symbolId=1           symbolId=2           symbolId=3
 (现有 2.0 节点，无内核改动)
```

每个分片进程内部：**不变** — 单 ring、单 WAL、单 HA 复制组。

## 4. 交付拆分（建议实现顺序）

### Phase A — 设计与契约（3.0.0-alpha）

- [ ] 本 ADR + [MIGRATION-3.0.md](../MIGRATION-3.0.md)
- [ ] 路由表键空间规范（`shardKey`、`symbolId`、primary 端点、generation）
- [ ] OpenAPI / 内部 JSON 运维 API 草案（register、lookup、drain）

### Phase B — `matcher-orchestrator` 模块（3.0.0-beta）

- [x] 模块 `matcher-orchestrator`：`OrchestratorStore`、`RoutingTable`、内存实现
- [x] etcd 写路径：`EtcdOrchestratorStore`（register / bind / drain / lookup）
- [x] 单测 + fake etcd 集成测
- [ ] 只读 watch 推送（当前为 `RoutingTable.refresh()` 拉取）
- [ ] 三 shard lab 脚本扩展

### Phase C — SDK / 运维（3.0.0）

- [ ] Java SDK：`MatcherClient` 可选 **orchestrator 感知**（自动 pick 端点）
- [ ] 移除 `matcher.clusterName`（2.x 已弃用）
- [ ] Runbook：滚动新增 shard、drain、故障域

### Phase D — 与 2.1 剩余项的关系

| 原 2.1 项 | 3.0 策略 |
| --- | --- |
| WAL 冷备 | **已完成**（[wal-archive-cold-backup.md](../operations/wal-archive-cold-backup.md)） |
| 传输语义统一 | 3.0 **不阻塞**；在 3.0.x 或 Phase D 并行 |
| `AERON_PREVIEW` 移除 | 保持 `@Deprecated` 直至传输统一完成 |

## 5. 控制面数据模型（草案）

```text
/ull-matcher/v3/routes/symbols/{symbolId}     → { shardKey, generation, updatedAt }
/ull-matcher/v3/shards/{shardKey}             → { nodeId, http, grpc, binary, role, state }
/ull-matcher/v3/shards/{shardKey}/assignments → { symbolId, ... }   # 1:1 在 3.0
```

与现有 etcd/ZK node registry **扩展前缀**，避免破坏 2.0 单分片部署（无 orchestrator 时行为不变）。

## 6. 质量门禁（3.0 GA）

- `./mvnw -Pstyle-check verify` 全绿
- 新增 orchestrator 模块 line ≥ 0.80 / branch ≥ 0.70（与父 POM 一致）
- Lab：至少 3 shard × 1P2S failover smoke + 路由切换测试
- 文档：MIGRATION-3.0、production-deployment 增补「多 shard 拓扑」

## 7. 关联文档

- [shard-model-design.md](shard-model-design.md)
- [shard-capacity-planning.md](shard-capacity-planning.md)
- [replication-transport-2.1.md](replication-transport-2.1.md)
