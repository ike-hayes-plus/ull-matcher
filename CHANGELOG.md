# Changelog

本文件记录 ull-matcher 的所有重要变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Changed

- **BREAKING** HA 权威快照（周期 RDB、`POST /api/v1/admin/snapshot`、gRPC/Aeron 导出）仅 PRIMARY 可创建或流出。
- **BREAKING** 删除从未由网关返回的 `SubmitResult.RING_FULL_AFTER_WAL_APPEND` 及 binary status `5`。
- **BREAKING** standby `installSnapshotFrom` 与 fenced rejoin 一样隔离本地 WAL，不再重放快照序列号之后的本地尾部。
- **BREAKING** 启动恢复在快照存在且本地仍有 WAL 分段时必须校验 checkpoint manifest。
- **BREAKING** FOK 不可全成改为 `REJECTED` / `FOK_NOT_FILLABLE`；容量拒绝统一为 `REJECTED` 并计入 `rejectedCommandCount`。
- **BREAKING** 编排 `lookupRoute` 在 shard 缺失时返回空；进程关闭时删除 shard 与 symbol 路由，不再只标 drain。
- **BREAKING** 删除与 `/api/v1/runtime/health` 重复的 `/api/v1/runtime/state`。
- `nodeId` 拒绝包含 `|`，避免 etcd/ZK 租约 payload 被拆错。
- `AsyncEventDispatcher` 补齐订单事件的 `side` / `orderType` / `timeInForce` / `price` / `quantity`。
- 撤单成功事件带上簿上订单的 side / type / tif / price / quantity。
- Java SDK 增加 `MatcherBinaryClient.submitOrdersCommitted`（binary frame type 3）。

## [3.0.0] - 2026-10-06

**3.0 绿田基线**（无历史版本迁移路径）。坐标与快速开始见 [README.md](README.md)。

### Added

- 模块 `matcher-net`：`MatcherHttpTransport`（JDK HttpClient NIO、默认 HTTP/2、共享虚拟线程 executor）。
- 多分片编排：`matcher-orchestrator`、`EtcdOrchestratorStore`；`-Dmatcher.orchestratorEnabled=true` / `ull.matcher.orchestrator-enabled` 自注册与 heartbeat（须 etcd 控制面）。
- 只读编排 HTTP `GET /api/v1/orchestrator/routes/symbols/{symbolId}`；SDK `MatcherOrchestratorClient`。
- HTTP ingress：进程级共享虚拟线程 `MatcherHttpExecutors` + route 超时调度；默认 budget 2048/1024/512。
- `RoutingTable.startBackgroundRefresh` 供网关/SDK 周期性刷新路由缓存。
- 持久化预设 `matcher.persistenceProfile` 与周期快照（见 [persistence.md](doc/operations/persistence.md)）。
- WAL 冷备：`matcher.walColdArchiveDir`（见 [doc/operations/wal-archive-cold-backup.md](doc/operations/wal-archive-cold-backup.md)）。

### Changed

- etcd `isHeldBy` 每次读 etcd，去掉丢租约后仍放行写入的本地正向缓存。
- 快照成功后归档并删除已被覆盖的热 WAL 分段，同时写 checkpoint manifest。
- `scripts/deploy/default.conf` 的 binary 默认绑定改回 `127.0.0.1`，与 HTTP / PROD 闸门一致。
- Maven 全仓库版本 **3.0.0**；构建 JDK 25 / Maven 4。
- 配置仅 **`matcher.cluster`**（移除 `matcher.clusterName`）；TLS 仅 **`matcher.transportTls*`**（移除 grpc 前缀别名）。
- HTTP 写/读统一 `HttpRequestPipeline.blocking` + 虚拟线程 dispatch。
- CI 挂 `validate-benchmark-baseline.py --skip-missing-reports`（不跑 3.0-full matrix）。

### Removed

- 历史版本迁移文档与 `HttpRequestPipeline.directBlocking`。
- 架构文档统一为 `replication-transport.md`、`shard-orchestration.md`（无版本后缀 ADR 文件名）。
