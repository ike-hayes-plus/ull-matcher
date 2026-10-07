# Changelog

本文件记录 ull-matcher 的所有重要变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Changed

- 部署脚本把 `::1` 当作本机，启动前检查 Aeron 端口占用；实验室停节点也会收 Aeron 端口。
- `cluster.conf.example` 远程生产控制面改为 etcd https；ZooKeeper 示例改回 loopback。
- standby 入环在环满时 1 秒后失败；等撮合线程 applied 只在关闭时退出，不再把「先 durable 后 apply」当成故障。
- WAL 打开时把只有魔数、字段全 0 的半写尾记录当成崩溃尾，不再在 `Side.from(0)` 上炸掉。
- **BREAKING** PROD 拒绝非 loopback ZooKeeper 连接串。远程控制面走 etcd https；本仓库尚未实现 ZooKeeper TLS。
- **BREAKING** 未写入 WAL 的提交不再消耗命令序列号；WAL 追加成功后的命令一定会进入撮合环。单节点 `LOCAL_ONLY` 在本地接受时直接进入 `COMMITTED`。
- **BREAKING** `HttpSubmitAckMode` 成为 `MatcherServerConfig` 字段。`ull.matcher.http-submit-ack-mode` 写入该字段，`auto-start=false` 时用配置构造的 `MatcherServerApp` 同样生效。
- CI 用 `doc/operations/benchmark-reports/` 对照主表数字；缺报告即失败。
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

### Removed

- **BREAKING** 删除 `ReplicationTransportType.AERON_PREVIEW`、preview ingress / reconciliation，以及 `allowPreviewTransportInProd`。Aeron 目录、端口、stream 配置保留为 `AeronTransportConfig`（`matcher.aeron*` / `ull.matcher.cluster.aeron`）。传输计数改名为 `published*` / `received*`；删除始终为 0 的 preview 序列、gap、乱序指标。
- 删除未接入生产路径的 `ReplicatedWalWriter`。它的 `force()` 只刷本地 WAL，和当前异步复制链路重复。

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
