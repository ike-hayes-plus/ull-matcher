# Changelog

本文件记录 ull-matcher 的所有重要变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [3.0.0] - 2026-10-06

**绿田 3.0 基线**（无 1.x/2.x 迁移路径）。集成说明见 [doc/INTEGRATION.md](doc/INTEGRATION.md)。

### Added

- 模块 `matcher-net`：`MatcherHttpTransport`（JDK HttpClient NIO、默认 HTTP/2、共享虚拟线程 executor）。
- 多分片编排：`matcher-orchestrator`、`EtcdOrchestratorStore`；`-Dmatcher.orchestratorEnabled=true` 自注册与 heartbeat。
- 只读编排 HTTP `GET /api/v1/orchestrator/routes/symbols/{symbolId}`；SDK `MatcherOrchestratorClient`。
- HTTP ingress：进程级共享虚拟线程 `MatcherHttpExecutors` + route 超时调度；默认 budget 2048/1024/512。
- `RoutingTable.startBackgroundRefresh` 供网关/SDK 周期性刷新路由缓存。
- WAL 冷备：`matcher.walColdArchiveDir`（见 [doc/operations/wal-archive-cold-backup.md](doc/operations/wal-archive-cold-backup.md)）。

### Changed

- Maven 全仓库版本 **3.0.0**；构建 JDK 25 / Maven 4。
- 配置仅 **`matcher.cluster`**（移除 `matcher.clusterName`）；TLS 仅 **`matcher.transportTls*`**（移除 grpc 前缀别名）。
- HTTP 写/读统一 `HttpRequestPipeline.blocking` + 虚拟线程 dispatch。

### Removed

- 文档与代码中的 2.0 迁移、`MIGRATION-2.0.md`、`HttpRequestPipeline.directBlocking`。

### Fixed

- HA / WAL / ingress 与 2.0 tag 以来累积修复保持有效（详见 git history `v2.0.0..HEAD`）。
