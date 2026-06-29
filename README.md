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
- 提交链是：
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
| 1P1S REST HA       | REST 写入 + 主备复制 | REST           | `GRPC` 或 `AERON` | 普通业务高可用入口    |
| 1P1S binary HA     | 高频入口 + 主备闭环 | Binary ingress | `GRPC` 或 `AERON` | 高频生产入口的目标部署模式 |

### 传输选择

- 默认传输：`GRPC`
- 可选高性能传输：`AERON`
默认 `GRPC` 是保守生产默认值。这表示它在基线事实源中单备 committed 曲线最稳定，不表示所有拓扑下都绝对最快。

生产环境一次只选择一种权威复制传输。不要在生产上同时依赖两种传输。

这里的 `REST HA` 表示外部客户端通过 REST 调用 primary 节点，节点之间的主备复制仍然只使用 `GRPC` 或 `AERON`。HTTP 不用于主备同步。

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

`BinaryOrderIngressServer` 是高频主入口。支持：

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

README 只保留容量判断所需的摘要。完整 benchmark 口径和可复现脚本说明见：
- [Benchmark 基线](doc/operations/benchmark-baseline-current.md)

### 统一场景口径

除非单项 benchmark 另有说明，标准 crossing 场景使用：

- `restingOrders = 2048`
- `crossingOrders = 2048`
- `concurrency = 24`
- `batchSize = 64`（binary ingress 场景）

`restingOrders = 2048` 不是业务上限。它只是稳定 benchmark 口径：用 2048 笔预挂卖单形成足够深的可成交盘口，再用 2048 笔 crossing 买单完全吃掉该盘口。2048 是 2 的幂，便于固定批次、重复运行和观察队列/复制水位。

### 容量摘要

| 层级 | 推荐用途 | 基线口径 | 读数重点 |
| --- | --- | --- | --- |
| Core-only matcher | 判断撮合主链是否是瓶颈 | 单 JVM，仅 `UltraLowLatencyMatcher.onCommand(...)` | 微秒级延迟，不代表服务吞吐 |
| Single-node HTTP | 管理面、普通业务入口 | REST + WAL + 本地提交链 | 通用入口容量 |
| Single-node binary | 高频单节点入口 | Binary ingress + WAL + 本地提交链 | 高频入口容量 |
| JVM 内 direct HA | 快速验证 HA 主链 | Binary ingress + JVM 内 standby | 排除外部进程噪声 |
| External REST HA | 普通业务高可用入口 | 外部客户端 REST 写入 + 内部 `GRPC`/`AERON` 复制 | 看 `trade events/s` 和 `replication committed/s` |
| External binary HA committed | 生产容量规划 | 外部多进程 + WAL + 复制确认 | 看 `replication committed/s` |

结论：

- 撮合核心不是主瓶颈；服务入口、WAL force、复制确认和外部进程调度才是容量规划重点。
- 高频主入口应优先使用 binary ingress；REST 不代表高频交易入口上限。
- REST HA 的 REST 只表示外部写入协议，主备同步不走 HTTP。
- 生产容量规划看 **replication committed throughput**，不要只看 accepted throughput。
- 单备生产默认仍建议 `GRPC`；`AERON` 是高性能可选项，多备拓扑需要结合 soak/chaos 结果判断。

## 推荐配置与容量参考

### 入口选择

- 管理面、查询、补单、普通业务接入：REST
- 高频策略、低延迟接单、批量命令流：Binary ingress

### 推荐服务器配置

压测基线来自 Apple M4 Pro、12 逻辑 CPU、24 GiB 内存、本地 SSD、JDK 21。生产环境建议按下面配置起步：

| 用途 | CPU | 内存 | 磁盘 | 网络 |
| --- | --- | --- | --- | --- |
| REST 普通业务入口 | 8 核以上高主频 CPU | 16 GiB 以上 | NVMe SSD | 10 Gbps |
| Binary 高频入口 | 16 核以上高主频 CPU | 32 GiB 以上 | 低延迟 NVMe SSD | 10 Gbps 以上 |
| 多分片或多备部署 | 按分片独占 CPU 预算 | 64 GiB 以上 | 独立 NVMe 或隔离卷 | 25 Gbps 以上 |

优先保证 CPU 主频、磁盘 fsync 延迟和网络尾延迟。单机堆更多不能替代分片扩容。

### 参考机器上的保守容量参考

下表是容量规划的保守值，不是单次 benchmark 峰值。`trade events/s` 表示压测期间每秒完成的撮合成交事件数。完整事实源见
[Benchmark 基线](doc/operations/benchmark-baseline-current.md)。

| 模式                                                            | 推荐用途                             | accepted/s | trade events/s | committed/s |
| ------------------------------------------------------------- | -------------------------------- | ----------: | --------------: | ----------: |
| Core-only matcher                                             | 纯内存撮合主链                         | `24,855,862` | `N/A` | `N/A` |
| Single-node HTTP                                              | 通用业务服务                           | `6,532` | `6,532` | `6,532` |
| Single-node binary                                            | 高频单节点                            | `132,048` | `132,048` | `132,048` |
| External `1P1S` REST + `GRPC`                                 | REST 写入 + 单备复制                    | `4,250` | `4,250` | `4,240` |
| External `1P1S` REST + `AERON`                                | REST 写入 + 单备复制                    | `4,158` | `4,158` | `4,148` |
| External `1P1S` binary + `GRPC` replication committed         | 高频主入口 + 单备闭环真实 committed         | `118,535` | `118,535` | `107,297` |
| External `1P1S` binary + `AERON` replication committed        | 高频主入口 + 单备闭环真实 committed         | `103,706` | `103,706` | `92,479` |
| External `1P2S` binary + `GRPC` quorum replication committed  | 高频主入口 + 两备 quorum 闭环真实 committed | `90,381` | `90,381` | `83,693` |
| External `1P2S` binary + `AERON` quorum replication committed | 高频主入口 + 两备 quorum 闭环真实 committed | `119,574` | `119,574` | `71,271` |
| External `1P3S` binary + `GRPC` quorum replication committed  | 高频主入口 + 三备 quorum 闭环真实 committed | `60,546` | `60,546` | `57,556` |
| External `1P3S` binary + `AERON` quorum replication committed | 高频主入口 + 三备 quorum 闭环真实 committed | `82,832` | `82,832` | `14,390` |

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

- 入门与贡献：
  - [贡献指南](CONTRIBUTING.md)
  - [安全策略](SECURITY.md)
- 架构：
  - [Shard 模型设计](doc/architecture/shard-model-design.md)
  - [多分片容量规划](doc/architecture/shard-capacity-planning.md)
  - [Spring Boot Starter 设计](doc/architecture/matcher-spring-boot-starter-design.md)
- 运维与压测：
  - [生产部署与容量总览](doc/operations/production-deployment-and-capacity.md)
  - [Benchmark 基线](doc/operations/benchmark-baseline-current.md)
  - [部署模式](doc/operations/deployment-modes.md)
  - [HA / Sharding 实验手册](doc/operations/ha-sharding-lab.md)
  - [HTTP 路由预算与可观测性](doc/operations/http-route-budget-observability.md)
  - [WAL / 复制 / 租约 / 混沌矩阵](doc/operations/wal-replication-lease-chaos-matrix.md)
  - [生产灰度发布 Runbook](doc/operations/gray-release-runbook.md)
  - [生产灰度发布检查清单](doc/operations/gray-release-checklist.md)
- 实验性 `AERON_PREVIEW`、transport 切换窗口和预览验证请看 [HA / Sharding 实验手册](doc/operations/ha-sharding-lab.md)

## 最后说明

这个项目的目标是把单分片撮合节点做到可嵌入、可部署、可恢复、可复制、可观测。

如果你的目标是更大的总吞吐，正确做法是：

1. 先把单分片做实
2. 再按交易对或业务维度分片
3. 用真实 benchmark 验证总容量

不要把单个 HTTP 接口的吞吐，误认为整个交易系统的最终上限。
