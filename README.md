# ull-matcher

`ull-matcher` 是一个面向单交易对 / 单分片的超低延迟撮合内核与高可用节点实现。它既可以作为 Java 库嵌入上游系统，也可以作为独立服务进程部署。

它解决的是撮合节点本身的问题：
- 高吞吐接单
- 单线程撮合
- WAL 持久化
- 快照与重放
- 主备复制与自动接管
- 健康检查、readiness 和 Prometheus 指标

它不负责账户、清结算、风控、统一网关、行情总线。这些应该部署在它的上游或下游。

## What It Is

- 单线程撮合核心，热路径避免锁竞争
- 同时支持库嵌入和独立服务部署
- 内置 WAL、快照、重放和主备复制
- 同时提供通用 REST 入口和高频 binary ingress
- 复制传输支持 `GRPC`、`AERON`
- 仓库内自带 benchmark、lab、deploy、ops、chaos 脚本

## What It Is Not

- 不是账户系统
- 不是清结算系统
- 不是风控系统
- 不是行情分发系统
- 不是统一 API 网关
- 不是多交易对混跑的大一统状态机

## Core Model

- 一个 shard 对应一个交易对或一个明确分片
- shard 状态由单线程持有
- 最后一跳仍然是单消费者 ring
- 高频主入口是 binary ingress
- 通用业务入口是 HTTP/REST
- 当前提交链是：
  1. 入口接收请求
  2. 多生产者聚合进入 submit queue
  3. 预分配命令槽位填充命令
  4. 先写 WAL
  5. 再进入撮合 ring
  6. 复制确认异步推进
  7. 通过 submission 查询接口获取最终复制确认状态
- 高可用模型是 `1 primary + N standbys`

## 快速开始

### 环境要求

- JDK 21
- Maven 3.9+
- 本地 SSD / NVMe

工程内已经带 `.sdkmanrc`，请在仓库目录执行：

```bash
sdk env
java -version
```

不要直接使用系统默认 Java。

### 构建

```bash
mvn --batch-mode --no-transfer-progress test
```

### 最短单机启动

```bash
sdk env
java -Dmatcher.nodeId=node-a \
     -Dmatcher.symbolId=1 \
     -Dmatcher.dataDir=target/matcher-server \
     -cp "$(cat target/lab/classpath.txt):matcher-server/target/classes:matcher-core/target/classes:matcher-storage/target/classes:matcher-runtime/target/classes:matcher-ha/target/classes:matcher-ha-grpc/target/classes:matcher-ha-aeron/target/classes:matcher-ha-zookeeper/target/classes:matcher-discovery-zookeeper/target/classes:matcher-discovery-nacos/target/classes:matcher-spring-boot-starter/target/classes:matcher-examples/target/classes" \
     io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain
```

默认会开放：

- HTTP: `127.0.0.1:8080`
- gRPC replication: `127.0.0.1:9190`

### 最短一主一备启动

先起 lab 依赖：

```bash
./scripts/chaos/lab.sh up
```

再起两个节点：

```bash
REPLICATION_TRANSPORT=GRPC bash scripts/deploy/start-node.sh node-a 8080 9190 15090 10080
REPLICATION_TRANSPORT=GRPC bash scripts/deploy/start-node.sh node-b 8081 9191 15091 10081
```

`deploy` 脚本默认使用：

- 数据目录：`var/data`
- 日志目录：`var/log`
- 运行模式：`PROD`

检查主节点：

```bash
curl http://127.0.0.1:8080/api/v1/runtime/health
curl http://127.0.0.1:8080/api/v1/runtime/readiness
```

停节点：

```bash
bash scripts/deploy/stop-node.sh node-a
bash scripts/deploy/stop-node.sh node-b
```

## 部署模式

| 模式                 | 用途          | 入口             | 复制               | 说明            |
| ------------------ | ----------- | -------------- | ---------------- | ------------- |
| Embedded core      | 纯撮合内核容量验证   | 直接库调用          | 无                | 用来测单分片内核上限    |
| Single-node HTTP   | 单节点通用服务     | REST           | 无                | 适合管理面、普通业务接入  |
| Single-node binary | 单节点高频入口     | Binary ingress | 无                | 适合低延迟接单       |
| 1P1S HTTP HA       | 外部进程主备服务    | REST           | `GRPC` 或 `AERON` | 当前最稳的保守部署模式   |
| 1P1S binary HA     | 高频入口 + 主备闭环 | Binary ingress | `GRPC` 或 `AERON` | 高频生产入口的目标部署模式 |

### 传输选择

- 默认传输：`GRPC`
- 可选高性能传输：`AERON`
默认 `GRPC` 是当前保守生产默认值。这表示它在 current 事实源中单备 committed 曲线最稳定，不表示所有拓扑下都绝对最快。

生产环境一次只选择一种权威复制传输。不要在生产上同时依赖两种传输。

## 脚本分层

- `scripts/deploy/`: 正式部署入口
  - 也包含灰度前后集群采样脚本
- `scripts/bench/`: 吞吐、延迟、committed 专项压测
- `scripts/lab/`: 本地实验拓扑和临时压测编排
- `scripts/ops/`: 运维采样和状态收集
- `scripts/chaos/`: 混沌验证和 failover 演练

## 对外接口

### REST 接口

| 方法     | 路径                                                      | 用途                |
| ------ | ------------------------------------------------------- | ----------------- |
| `POST` | `/api/v1/orders`                                        | 新单                |
| `POST` | `/api/v1/orders/cancel`                                 | 撤单                |
| `GET`  | `/api/v1/orders/{orderId}`                              | 查询订单状态            |
| `GET`  | `/api/v1/submissions/{submissionId}`                    | 查询 submission     |
| `GET`  | `/api/v1/submissions/by-idempotency?idempotencyKey=...` | 按幂等键查询 submission |
| `POST` | `/api/v1/admin/snapshot`                                | 触发快照              |
| `GET`  | `/api/v1/runtime/health`                                | 节点健康              |
| `GET`  | `/api/v1/runtime/readiness`                             | readiness         |
| `GET`  | `/api/v1/runtime/state`                                 | 运行态               |
| `GET`  | `/metrics`                                              | Prometheus 指标     |

### Binary ingress

`BinaryOrderIngressServer` 是高频主入口。当前支持：

- 批量新单帧
- 批量撤单帧
- 固定帧响应

建议：
- REST 用于管理面、查询、补单、普通业务接入
- Binary ingress 用于高频接单、低延迟策略接入、批量命令流

## 订单生命周期与返回语义

### HTTP 返回语义

`POST /api/v1/orders` 和 `POST /api/v1/orders/cancel` 返回的是 **提交受理状态**，不是最终成交结果。

语义分两层：

- `202`
  - 本地已受理
  - 已进入本地可恢复状态
  - 复制确认仍可能进行中
- `200`
  - 已达到 replication committed

### 幂等

HTTP 写接口支持 `Idempotency-Key`。

推荐做法：

- 每笔业务命令都带 `Idempotency-Key`
- 写后用 `submissionId` 或 `Idempotency-Key` 查询最终状态

### 查询接口

```bash
curl http://127.0.0.1:8080/api/v1/submissions/{submissionId}
curl "http://127.0.0.1:8080/api/v1/submissions/by-idempotency?idempotencyKey=your-key"
```

### TTL 事件

TTL 到期不会静默丢弃。TTL 处理会发事件，和普通订单事件一样可被上游感知。

## 高可用与控制面

支持的拓扑：
- `1P1S`
- `1P2S`
- `1P3S`

核心角色：
- `PRIMARY`
- `STANDBY`
- `CATCHING_UP`
- `FENCED`

控制面负责：
- 选主与接管
- catch-up 与快照同步
- 复制状态观测
- readiness gating
- transport policy 与 TLS reload

## 端口规划

首页只保留默认端口段：

- HTTP: `8080+`
- gRPC replication: `9190+`
- Aeron replication: `15090+`
- Binary ingress: `10080+`
- Prometheus: `19091`

完整端口矩阵见 [HA / Sharding 实验手册](doc/operations/ha-sharding-lab.md)。

## 监控与排障

最常用的排障入口只有三个：

- `GET /api/v1/runtime/health`
- `GET /api/v1/runtime/readiness`
- `GET /metrics`

优先看这些字段：

- `role`
- `acceptingClientCommands`
- `clientTrafficReady`
- `replicationQueueDepth`
- `replicationLastBatchSize`
- `lastDurableSequence`
- `lastAppliedSequence`
- `standbyApplyQueueDepth`
- `standbyAckLastFlushMicros`

更完整的健康字段、指标和排障步骤见：
- [生产部署与容量总览](doc/operations/production-deployment-and-capacity.md)
- [HTTP 路由预算与可观测性](doc/operations/http-route-budget-observability.md)

## 压测场景与实测数据

### 压测机器

- Apple M4 Pro
- 12 逻辑 CPU
- 24 GiB 内存
- JDK 21

### 统一场景口径

除非另有说明，下面的数据都使用：

- `restingOrders = 2048`
- `crossingOrders = 2048`
- `concurrency = 24`
- `batchSize = 64`（适用于 binary ingress 场景）

### 已发布的稳定场景

README 只保留当前稳定结论。明细产物、归档和实验说明统一放在：
- [当前稳定基线归档](doc/operations/benchmark-baseline-current.md)

| 场景                                      | 入口/拓扑                                            | accepted orders/s | trade events/s | replication committed/s | p99 latency |
| --------------------------------------- | ------------------------------------------------ | ----------------- | -------------- | ----------------------- | ----------- |
| Core-only matcher                       | 单 JVM，只测 `UltraLowLatencyMatcher.onCommand(...)` | `6,992,019.06`    | `2,749,366.78` | `N/A`                   | `0.46 µs`   |
| Embedded journaled core                 | 单 JVM，带 WAL 与 event dispatcher                   | `48,977.35`       | `48,977.35`    | `N/A`                   | `N/A`       |
| Single-node HTTP                        | 单节点，REST                                         | `2,404.75`        | `2,404.75`     | `2,404.75`              | `94.32 ms`  |
| Single-node binary                      | 单节点，Binary ingress                               | `20,075.79`       | `20,075.79`    | `20,075.79`             | `3.74 ms`   |
| JVM 内 direct HA                         | `1P1S`，Binary ingress，JVM 内 direct standby       | `31,296.12`       | `31,296.12`    | `31,296.12`             | `1.72 ms`   |
| External `1P1S` binary + `GRPC`         | Binary ingress + 外部进程主备                          | `14,297.82`       | `14,297.82`    | `14,297.82`             | `2.15 ms`   |
| External `1P1S` binary + `AERON`        | Binary ingress + 外部进程主备                          | `12,731.49`       | `12,731.49`    | `12,731.49`             | `2.60 ms`   |
| External `1P2S` binary + `GRPC` quorum  | Binary ingress + 外部进程一主两备                        | `12,486.44`       | `12,486.44`    | `12,486.44`             | `3.01 ms`   |
| External `1P2S` binary + `AERON` quorum | Binary ingress + 外部进程一主两备                        | `15,243.83`       | `15,243.83`    | `15,243.83`             | `1.71 ms`   |
| External `1P3S` binary + `GRPC` quorum  | Binary ingress + 外部进程一主三备                        | `10,791.67`       | `10,791.67`    | `10,791.67`             | `3.42 ms`   |
| External `1P3S` binary + `AERON` quorum | Binary ingress + 外部进程一主三备                        | `12,373.05`       | `12,373.05`    | `12,373.05`             | `2.80 ms`   |
| External `1P1S` + `GRPC`                | REST + 外部进程主备                                    | `1,398.13`        | `1,398.13`     | `1,391.99`              | `24.62 ms`  |
| External `1P1S` + `AERON`               | REST + 外部进程主备                                    | `1,228.36`        | `1,228.36`     | `1,220.56`              | `28.08 ms`  |

### 复制确认场景（Binary ingress 外部 HA）

| 场景                                      | accepted orders/s | replication committed/s | catch-up   | p99 latency |
| --------------------------------------- | ----------------- | ----------------------- | ---------- | ----------- |
| External `1P1S` binary + `GRPC`         | `21,648.10`       | `19,935.02`             | `0.0081 s` | `2.48 ms`   |
| External `1P1S` binary + `AERON`        | `17,359.70`       | `16,136.80`             | `0.0089 s` | `3.13 ms`   |
| External `1P2S` binary + `GRPC` quorum  | `17,739.98`       | `16,495.97`             | `0.0087 s` | `3.06 ms`   |
| External `1P2S` binary + `AERON` quorum | `12,319.45`       | `10,404.45`             | `0.0306 s` | `4.61 ms`   |
| External `1P3S` binary + `GRPC` quorum  | `13,697.46`       | `12,887.54`             | `0.0094 s` | `4.17 ms`   |
| External `1P3S` binary + `AERON` quorum | `27,829.30`       | `21,109.76`             | `0.0234 s` | `2.19 ms`   |

### 如何理解这些数字

- `Core-only matcher` 只测 `UltraLowLatencyMatcher` 主链，不包含网络、HTTP、WAL、HA、IPC 和控制面。这是当前仓库里最接近 `exchange-core` README 延迟表的口径。
- `Embedded journaled core` 不包含 HTTP、控制面和外部进程边界，但仍包含本地 WAL 和事件分发。
- `Single-node HTTP` 只代表通用服务入口，不代表高频主入口上限。
- `Single-node binary` 代表当前单分片高频入口能力。
- `External ... replication committed` 才代表带 HA 持久化闭环的真实可交付吞吐。
- `accepted orders/s` 看入口受理能力；`replication committed/s` 看高可用真实上限。
- 明细事实源和历史归档统一放在 [benchmark-baseline-current.md](doc/operations/benchmark-baseline-current.md)。

### 不含闭环 vs 含 HA 闭环

这个对比只说明三件事：
- matcher core 不是当前主瓶颈；
- 高频入口应该优先使用 binary ingress；
- 真实生产主问题是 `accepted -> committed` 的折损，而不是撮合器本体。

如果只看撮合主链，当前已经进入百万级 ops/s 和微秒级延迟量级；如果把外部进程、持久化和 HA committed 算进去，当前最强单分片链路是 `binary + GRPC + 1P1S replication committed`，稳定在 `~19.9k orders/s`。

## 推荐配置与容量参考

### 入口选择

- 管理面、查询、补单、普通业务接入：REST
- 高频策略、低延迟接单、批量命令流：Binary ingress

### 当前机器上的保守容量参考

| 模式                                                            | 推荐用途                             | 当前机器保守参考              |
| ------------------------------------------------------------- | -------------------------------- | --------------------- |
| Single-node HTTP                                              | 通用业务服务                           | `~2.4k orders/s`      |
| Single-node binary                                            | 高频单节点                            | `~20k orders/s`       |
| External `1P1S` binary + `GRPC` replication committed         | 高频主入口 + 单备闭环真实 committed         | `~19.5k–20k orders/s` |
| External `1P1S` binary + `AERON` replication committed        | 高频主入口 + 单备闭环真实 committed         | `~16k orders/s`       |
| External `1P2S` binary + `GRPC` quorum replication committed  | 高频主入口 + 两备 quorum 闭环真实 committed | `~16k–16.5k orders/s` |
| External `1P2S` binary + `AERON` quorum replication committed | 高频主入口 + 两备 quorum 闭环真实 committed | `~10k–10.5k orders/s` |
| External `1P3S` binary + `GRPC` quorum replication committed  | 高频主入口 + 三备 quorum 闭环真实 committed | `~12.5k–13k orders/s` |
| External `1P3S` binary + `AERON` quorum replication committed | 高频主入口 + 三备 quorum 闭环真实 committed | `~21k orders/s`       |
| External `1P1S` + `GRPC`                                      | REST 主备服务                        | `~1.3k–1.4k orders/s` |
| External `1P1S` + `AERON`                                     | REST 主备服务                        | `~1.2k orders/s`      |

### 推荐部署原则

- 把单交易对 / 单分片做成独立 shard
- 高频主入口优先使用 binary ingress
- REST 只承载管理面和普通业务面
- 生产一次只选择一种复制传输
- 扩容优先靠分片，不靠把所有交易对塞进一个状态机

默认建议：
- 单备生产默认：`GRPC`
- 高性能可选：`AERON`
容量规划和多分片估算见：
- [shard-capacity-planning.md](doc/architecture/shard-capacity-planning.md)

## 关键配置项

首页只保留最常用配置：

- 入口
  - `matcher.httpPort`
  - `matcher.binaryIngressEnabled`
  - `matcher.binaryIngressPort`
- 持久化
  - `matcher.walDurabilityMode`
  - `matcher.walForceBatchSize`
  - `matcher.walForceMaxDelayMicros`
- 复制
  - `matcher.replicationTransport`
  - `matcher.replicationTimeoutMillis`
- 传输安全
  - `matcher.transportMtlsRequired`
  - `matcher.transportTlsReloadMillis`

完整配置说明放在运维和架构文档中。

## 文档索引

- [生产部署与容量总览](doc/operations/production-deployment-and-capacity.md)
- [生产灰度发布 Runbook](doc/operations/gray-release-runbook.md)
- [生产灰度发布检查清单](doc/operations/gray-release-checklist.md)
- [发布说明模板](doc/operations/release-note-template.md)
- [首版发布说明草稿](doc/operations/release-notes-v0.1.0-draft.md)
- [当前稳定基线归档](doc/operations/benchmark-baseline-current.md)
- [部署模式](doc/operations/deployment-modes.md)
- [HA / Sharding 实验手册](doc/operations/ha-sharding-lab.md)
- [HTTP 路由预算与可观测性](doc/operations/http-route-budget-observability.md)
- [WAL / 复制 / 租约 / 混沌矩阵](doc/operations/wal-replication-lease-chaos-matrix.md)
- [多分片容量规划](doc/architecture/shard-capacity-planning.md)
- [Shard 模型设计](doc/architecture/shard-model-design.md)
- [Spring Boot Starter 设计](doc/architecture/matcher-spring-boot-starter-design.md)
- 实验性 `AERON_PREVIEW`、transport 切换窗口和预览验证请看 [HA / Sharding 实验手册](doc/operations/ha-sharding-lab.md)

## 最后说明

这个项目的目标是把单分片撮合节点做到可嵌入、可部署、可恢复、可复制、可观测。

如果你的目标是更大的总吞吐，正确做法是：

1. 先把单分片做实
2. 再按交易对或业务维度分片
3. 用真实 benchmark 验证总容量

不要把单个 HTTP 接口的吞吐，误认为整个交易系统的最终上限。
