# Changelog

本文件记录 ull-matcher 的所有重要变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

目标版本：**3.0.0**（跳过 2.1 独立发版线；2.0 单分片节点仍为稳定部署单元）。

### Added

- WAL 冷备（原 2.1）：`matcher.walColdArchiveDir` / `ull.matcher.wal-cold-archive-dir`（见 [doc/operations/wal-archive-cold-backup.md](doc/operations/wal-archive-cold-backup.md)）。
- 3.0 ADR：[doc/architecture/shard-orchestration-3.0.md](doc/architecture/shard-orchestration-3.0.md)、[doc/MIGRATION-3.0.md](doc/MIGRATION-3.0.md)（多分片编排；**非**单进程多 symbol）。
- 3.0 Phase B：`matcher-orchestrator` + `EtcdOrchestratorStore`（symbol 路由与 shard 注册/drain）。
- 3.0 Phase C（进行中）：`matcher-server` 可选 `matcher.orchestratorEnabled` 启动自注册与 heartbeat，关闭时 drain。
- 传输 ADR（后续 3.0.x）：[doc/architecture/replication-transport-2.1.md](doc/architecture/replication-transport-2.1.md)；`AERON_PREVIEW` 已 `@Deprecated`。

## [2.0.0] - 2026-10-05

2.0 基线：JDK 25 / Maven 4，生产安全默认值收敛；集成说明见
[doc/MIGRATION-2.0.md](doc/MIGRATION-2.0.md)（仅维护 2.0 服务端与 Java SDK）。

### Added

- 二进制入口连接治理：`matcher.binaryIngressMaxConnections`（默认 4096）、
  `matcher.binaryIngressHandshakeTimeoutMillis`（默认 5s）、
  `matcher.binaryIngressIdleTimeoutMillis`（默认 300s），并暴露
  `BinaryOrderIngressServer.connectionMetrics()` 用于观测拒绝 / 超时 / 回收计数。
- 免鉴权存活探针 `GET /api/v1/runtime/live`，只返回 `{"status":"UP"}`。
- `MatcherServerConfig.builder(...)` / `toBuilder()`，替代 30 余参数的规范构造函数。
- `MatcherClientConfig.ingressApiKeyHeader`，SDK 的 API key 请求头可配置。
- etcd mTLS 支持 PKCS#8 格式的 RSA / EC / Ed25519 私钥。
- WAL 崩溃一致性测试：fork 子 JVM 并在写入中途 `Runtime.halt()`，验证恢复后的尾部截断行为。
- HA 切主安全性测试：覆盖 fencing token 单调性、脑裂拒绝与租约过期切主。
- PIT 变异测试门槛（`-Pmutation`）、CycloneDX SBOM（`-Psbom`）、
  OWASP 依赖漏洞扫描（`-Pdependency-check`）、CodeQL 工作流。
- CI 增加 Linux + macOS 双平台矩阵。

### Changed

- **BREAKING** `/api/v1/runtime/health`、`/runtime/state`、`/runtime/readiness` 现在要求 API key。
  免鉴权探针请改用 `/api/v1/runtime/live`。
- **BREAKING** `matcher.ingressApiKeys` 非空时，二进制入口要求连接的前 32 字节为握手密钥。
  仅支持 2.0 客户端（含 binary 握手）；不提供 1.x SDK 兼容。
- **BREAKING** 依赖升级到 Jackson 3（`tools.jackson.*`）、Spring Boot 4.1.1、JUnit 6、
  gRPC 1.83.1、Protobuf 4.35.1、Aeron 1.53.3、OpenTelemetry 1.63.0。
- **BREAKING** 构建要求 JDK 25 与 Maven 4，仓库内提供 `./mvnw`。
- `matcher.clusterName` 更名为 `matcher.cluster`，旧名仍可用但会打印弃用告警。
- etcd 的「非 loopback 端点必须 https」校验从 `MatcherServerMain` 下沉到 `EtcdConfig`
  构造期，Spring Boot starter 路径同样生效。
- WAL 段文件元数据只在创建时 `force(true)` 一次，每条命令的 `force()` 只 msync 脏页。
- 覆盖率门槛统一为 line 0.80 / branch 0.70，取消 server 与 ha 模块的 0.50 特例。

### Fixed

- PROD 模式下 `matcher.dataDir` 的临时目录检测由前缀匹配改为逐段路径匹配，
  此前 `./x/target/wal` 之类的路径可以绕过检查。
- 故障切换时 fencing token 可能复用同一 epoch：晋升 epoch 原本取自被观测到的
  （可能已过期的）主节点令牌，导致反复切主时 epoch 停在同一个值，fencing 形同虚设。
  现在以租约存储中的当前 epoch 为准取 `max(proposed, current + 1)`。
- HTTP API key 比较改为常量时间且不短路，消除按字节的计时侧信道。
- `matcher-server` 与 `matcher-ha-aeron` 的 surefire `argLine` 覆盖会静默丢弃 JaCoCo agent，
  导致这两个模块的覆盖率门槛实际从未执行过。
- etcd mTLS 只配置证书链或只配置私钥时不再静默降级为单向 TLS，改为构造期报错。

## [1.1.0.0] - 2026-07-02

首个公开版本（`b3015cc`，Initial public release）。

[Unreleased]: https://github.com/ike-hayes-plus/ull-matcher/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/ike-hayes-plus/ull-matcher/compare/v1.1.0.0...v2.0.0
[1.1.0.0]: https://github.com/ike-hayes-plus/ull-matcher/releases/tag/v1.1.0.0
