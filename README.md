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

## 模块地图

仓库按撮合热路径、持久化、运行时、高可用、服务入口和集成工具分层。核心模块不依赖 server；server 负责把各层装配成可部署进程。

| 模块 | 作用 | 主要使用者 |
| --- | --- | --- |
| `matcher-core` | 撮合内核、订单簿、命令/事件对象池、热路径队列和 ring 基础结构 | 嵌入式撮合、runtime、benchmark |
| `matcher-storage` | WAL、分段 mmap 日志、快照落盘辅助、持久化同步语义 | runtime、server |
| `matcher-runtime` | WAL-before-ring 提交网关、match loop、异步事件分发、运行时快照 | server、测试和嵌入式运行 |
| `matcher-ha` | 高可用通用抽象：lease、fencing、replication、standby sync、snapshot sync、transport SPI | 各 HA 插件、server |
| `matcher-ha-zookeeper` | ZooKeeper lease store，实现 primary lease 与 fencing token 持有检查 | ZK 控制面部署 |
| `matcher-ha-etcd` | etcd lease store 与 etcd node registry，实现 etcd 控制面 | etcd 控制面部署 |
| `matcher-discovery-zookeeper` | ZooKeeper node registry，实现节点发现 | ZK 控制面部署 |
| `matcher-ha-grpc` | gRPC 复制协议、server/client、TLS 配置与 gRPC 传输指标 | `GRPC` 复制传输 |
| `matcher-ha-aeron` | Aeron 编解码、preview ingress、secure envelope、Aeron 传输指标 | `AERON` / `AERON_PREVIEW` 传输 |
| `matcher-server` | 独立节点进程：HTTP API、binary ingress、HA supervisor、transport provider 装配、readiness/metrics | 生产服务部署 |
| `matcher-server-dist` | shaded 可执行 JAR，入口为 `MatcherServerMain` | 发布和生产启动 |
| `matcher-sdk-java` | Java 客户端 SDK，封装 HTTP 与 binary ingress 协议 | 上游业务服务、策略服务 |
| `matcher-spring-boot-starter` | Spring Boot 自动配置，把 matcher 节点嵌入 Spring 应用 | Spring 内嵌部署 |
| `matcher-examples` | 可执行示例和脚本 classpath 辅助 | 快速体验、压测入口 |
| `matcher-benchmarks` | JMH benchmark | 核心性能回归与局部微基准 |

推荐阅读顺序：

1. 只关心撮合算法和单线程模型：`matcher-core` → `matcher-runtime`。
2. 关心生产部署：`matcher-server` → `matcher-server-dist` → `doc/operations/`。
3. 关心 HA：`matcher-ha` → `matcher-ha-grpc` / `matcher-ha-aeron` → `matcher-ha-zookeeper` / `matcher-ha-etcd`。
4. 关心接入：`matcher-sdk-java` 或 `matcher-spring-boot-starter`。

## Core Model

- 一个 shard 对应一个交易对或一个明确分片
- shard 状态由单线程持有
- 最后一跳仍然是单消费者 ring
- 高频主入口是 binary ingress
- 通用业务入口是 HTTP/REST
- Java SDK 复用服务端 HTTP / binary ingress 协议
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
mvn --batch-mode --no-transfer-progress -pl matcher-server-dist -am package
```

### 最短单机启动

```bash
sdk env
java -Dmatcher.nodeId=node-a \
     -Dmatcher.symbolId=1 \
     -Dmatcher.dataDir=target/matcher-server \
     -jar matcher-server-dist/target/ull-matcher-server-dist-1.1.0.0.jar
```

开发调试时也可以直接使用模块 classpath 启动：

```bash
java -Dmatcher.nodeId=node-a \
     -Dmatcher.symbolId=1 \
     -Dmatcher.dataDir=target/matcher-server \
     -cp "$(cat target/lab/classpath.txt):matcher-server/target/classes:matcher-core/target/classes:matcher-storage/target/classes:matcher-runtime/target/classes:matcher-ha/target/classes:matcher-ha-grpc/target/classes:matcher-ha-aeron/target/classes:matcher-ha-zookeeper/target/classes:matcher-ha-etcd/target/classes:matcher-discovery-zookeeper/target/classes:matcher-examples/target/classes" \
     io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain
```

默认会开放：

- HTTP: `127.0.0.1:8080`
- gRPC replication: `127.0.0.1:9190`

### 配置驱动集群启动

本地演示可以先起 lab 依赖：

```bash
./scripts/chaos/lab.sh up
```

生产环境应使用独立 ZooKeeper / etcd 等强一致控制面依赖，并在配置文件中设置可从所有节点访问的 `ZK_CONNECT` 或 `ETCD_ENDPOINT`。

生产部署建议使用配置文件驱动的集群入口。部署配置分两层：

- `scripts/deploy/default.conf`：基础默认值，包括 bind host、默认端口、WAL、复制、failover、TLS 和部署健康检查参数。
- `scripts/deploy/cluster.conf.example`：完整集群配置模板，只用于复制成真实环境配置，不应直接执行。

先复制模板：

```bash
mkdir -p conf
cp scripts/deploy/cluster.conf.example conf/merchant-42.env
```

然后只在 `conf/merchant-42.env` 中覆盖本环境需要改变的值，并为每个节点设置：

- `nodeId`
- 节点 IP / advertised host
- HTTP / gRPC / Aeron / binary 端口
- 精确 WAL / snapshot 数据目录
- 日志和 pid 目录
- `ZK_CONNECT` 或 `ETCD_ENDPOINT`
- `LEASE_PROVIDER`、`DISCOVERY_PROVIDER`
- `REPLICATION_TRANSPORT`、`REPLICATION_MODE`

一键启动、停止、重启、查看状态：

```bash
scripts/deploy/cluster.sh -c conf/merchant-42.env plan
scripts/deploy/cluster.sh -c conf/merchant-42.env validate
scripts/deploy/cluster.sh -c conf/merchant-42.env start
scripts/deploy/cluster.sh -c conf/merchant-42.env status
scripts/deploy/cluster.sh -c conf/merchant-42.env restart node-b
scripts/deploy/cluster.sh -c conf/merchant-42.env stop
```

`cluster.sh` 要求显式传入 `-c`，并拒绝直接使用 `scripts/deploy/cluster.conf.example`，避免把示例 IP、端口和目录当成真实生产配置执行。

`scripts/deploy/cluster.sh` 支持多机器部署：配置中的 `host` 如果不是本机，会通过 SSH 到 `REMOTE_ROOT` 执行同一套 `scripts/deploy/start-node.sh` / `stop-node.sh`。每台机器需要预先准备相同 release 目录、JDK、ZooKeeper 网络连通性、数据盘和日志目录权限。

控制面 provider 可独立切换：

```bash
# 默认：ZooKeeper lease + ZooKeeper discovery
LEASE_PROVIDER=zk
DISCOVERY_PROVIDER=zk
ZK_CONNECT=10.0.0.10:2181,10.0.0.11:2181,10.0.0.12:2181

# 可选：etcd lease + etcd discovery
LEASE_PROVIDER=etcd
DISCOVERY_PROVIDER=etcd
ETCD_ENDPOINT=http://10.0.0.10:2379,http://10.0.0.11:2379,http://10.0.0.12:2379
```

etcd provider 支持逗号分隔的多 endpoint，并按请求做 endpoint failover；生产环境应配置 3 个或更多 etcd 成员地址。提交入口的本地 primary lease 校验做短 TTL 正向缓存，默认 `matcher.etcdLocalHeldCheckCacheMillis=25`。它只缓存“当前节点仍持有当前 fencing token”的正向结果，用来避免每笔提交附近放大成 etcd HTTP 读；代价是提交入口感知 lease 丢失最多滞后这个缓存窗口，控制面 tick 仍会继续做权威续租和 fencing。

控制面只支持 `zk/zk` 或 `etcd/etcd` 两种生产组合。节点发现和 primary lease 使用同一种强一致控制面，避免发现、租约和 fencing 被拆散到不同系统。

默认 failover 是可用性优先：`FAILOVER_MIN_STANDBY_REPLICAS=1`。如果 `1P3S` 拓扑不希望只剩单节点时继续升主接客，可以设置 `FAILOVER_MIN_STANDBY_REPLICAS=2`，相当于多数派接管闸门；该检查在控制面 tick 上执行，不进入每笔订单热路径。

一主多备切换时，控制面会优先选择复制进度最优的 standby：先过滤不可达、不健康、超过最大 promotion lag 的节点，再按 `promotionWatermark=min(durable, applied)`、durable、applied、received 水位和 `nodeId` 稳定排序。胜出的 standby 还必须抢到外部 lease 才能提升为 primary；未胜出的备继续作为新主的复制目标提供 HA 能力。

旧主恢复或网络恢复时不会直接继续接单：primary 提交入口会低频校验本地 `nodeId + fencingToken` 是否仍持有 lease，默认最多每 `matcher.primaryLeaseSubmitCheckMicros=1000` 微秒检查一次。发现 lease 已属于新主时，本地 runtime 会立即 fence、停止接受写入并清空复制协调器，防止脑裂和乱接单；控制面 tick 仍会继续做常规续租 / fencing。

检查集群和主节点：

```bash
scripts/deploy/cluster.sh -c conf/merchant-42.env status
curl http://<primary-ip>:<http-port>/api/v1/runtime/readiness
```

停节点：

```bash
bash scripts/deploy/stop-node.sh node-a
bash scripts/deploy/stop-node.sh node-b
```

`start-node.sh` / `stop-node.sh` 是底层单节点工具；生产集群优先使用 `cluster.sh`，避免手工漏配 IP、端口、WAL 路径或复制模式。

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
  - 也包含 shard 发布前后集群采样脚本
- `scripts/bench/`: 吞吐、延迟、committed 专项压测
- `scripts/lab/`: 本地验证拓扑和压测编排
- `scripts/ops/`: 运维采样和状态收集
- `scripts/chaos/`: 混沌验证和 failover 演练

REST 压测支持单笔和批量两种模式。批量模式示例：

```bash
python3 scripts/bench/replication-commit-benchmark.py \
  --base-url http://127.0.0.1:8080 \
  --report target/current/http-ha/rest-batch-local.json \
  --ack-mode local \
  --http-submit-mode batch \
  --batch-size 32
```

连接已有集群时可使用封装入口，脚本只发起压测，不负责启动或停止节点：

```bash
scripts/lab/run-rest-commit-benchmark.sh \
  --base-url http://127.0.0.1:8080 \
  --standby-base-url http://127.0.0.1:8081 \
  --wait-for-ready-seconds 30 \
  --http-submit-mode single \
  --report target/current/http-ha/rest-single-1024-current.json

scripts/lab/run-rest-commit-benchmark.sh \
  --base-url http://127.0.0.1:8080 \
  --standby-base-url http://127.0.0.1:8081 \
  --wait-for-ready-seconds 30 \
  --http-submit-mode batch \
  --batch-size 32 \
  --report target/current/http-ha/rest-batch-1024-current.json
```

## Java SDK

Java SDK 模块：

- `matcher-sdk-java`

SDK 用于生产服务集成，封装 HTTP API 与 binary ingress 两类入口。HTTP 客户端适合管理面、查询、补单和普通业务接入：

```java
import io.github.ike.ullmatcher.sdk.MatcherClientConfig;
import io.github.ike.ullmatcher.sdk.MatcherHttpClient;
import io.github.ike.ullmatcher.sdk.NewOrderRequest;

MatcherHttpClient client = new MatcherHttpClient(MatcherClientConfig.localDefault());
client.submitOrder(NewOrderRequest.limit(7L, 1001L, "BUY", "GTC", 100L, 1L, "order-1001"));
client.submitOrders(java.util.List.of(
        NewOrderRequest.limit(7L, 1002L, "BUY", "GTC", 101L, 1L, "order-1002"),
        NewOrderRequest.limit(7L, 1003L, "BUY", "GTC", 102L, 1L, "order-1003")
));
```

binary ingress 客户端适合高频、低延迟、批量接单路径：

```java
import io.github.ike.ullmatcher.sdk.BinaryNewOrder;
import io.github.ike.ullmatcher.sdk.MatcherBinaryClient;

try (MatcherBinaryClient client = new MatcherBinaryClient("127.0.0.1", 18080, java.time.Duration.ofSeconds(2))) {
    client.submitOrders(java.util.List.of(BinaryNewOrder.buyLimit(7L, 1002L, 100L, 1L)));
}
```

`MatcherBinaryClient` 一个实例持有一条长连接，串行调用会复用同一个 socket。连接不可用时，下一次提交会先建立新连接；显式切换连接可调用 `reconnect()`。SDK 不会在提交失败后自动重放请求，因为连接中断时无法可靠判断服务端是否已经收到该批命令。业务侧需要基于 `orderId`、提交回执或幂等策略决定是否重试。

## 对外接口

### REST 接口

| 方法     | 路径                                                      | 用途                |
| ------ | ------------------------------------------------------- | ----------------- |
| `POST` | `/api/v1/orders`                                        | 新单                |
| `POST` | `/api/v1/orders/batch`                                  | 批量新单              |
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

`POST /api/v1/orders/batch` 使用一个 HTTP 请求提交多笔新单。它适合普通业务批量写入、补单和需要减少 REST 请求开销的场景，单次请求默认最多 `1024` 笔，可通过 `matcher.httpSubmitBatchMaxOrders` 调整。高频低延迟入口仍建议使用 binary ingress。

HTTP 写入支持 ack 模式：

- `local`
  - 默认快路径
  - 返回前只等待 primary 本地 WAL 与本机提交链受理
  - 复制确认可能仍在后台推进
- `committed`
  - 返回前等待所在集群 `matcher.replicationMode` 对应的 replication committed
  - 适合普通业务高可用写入和补单

默认模式可通过 `matcher.httpSubmitAckMode=local|committed` 配置。单次请求可用 JSON 字段 `ack`、查询参数 `?ack=...` 或请求头 `X-Ull-Ack` 覆盖。

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

### 多分片集群边界

HA 模型是“单 shard 一主多备”：一个交易对/商户分片内只有一个 primary 接受写入，standby 通过 WAL/复制接管。

多 shard 集群由外部路由层和部署编排层负责。项目内核保持“单 shard 一主多备”的低延迟模型，平台侧负责：

- shard slot / symbol 路由元数据
- 客户端 SDK 根据 slot 路由到对应 primary
- MOVED/ASK 类似的重定向语义
- shard 迁移、冻结写入、回放和切流协议
- 跨 shard 运维面：扩容、缩容、rebalance、故障域感知
- 每个 shard 内仍然保持一主多备复制和 failover

生产接入时优先采用静态多 shard 路由；在线迁移和自动 rebalance 应放在上层平台中实现。

## 端口规划

首页只保留默认端口段：

- HTTP: `8080+`
- gRPC replication: `9190+`
- Aeron replication: `15090+`
- Binary ingress: `10080+`
- Prometheus: `19091`

完整端口矩阵见 [HA / Sharding 验证环境手册](doc/operations/ha-sharding-lab.md)。

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
- [生产部署与容量规划](doc/operations/production-deployment-and-capacity.md)
- [HTTP 路由预算与可观测性](doc/operations/http-route-budget-observability.md)

## 压测场景与实测数据

README 只保留容量判断所需的摘要。完整 benchmark 口径和可复现脚本说明见：
- [Benchmark 基线](doc/operations/benchmark-baseline.md)

### 统一场景口径

除非单项 benchmark 另有说明，标准 crossing 场景使用：

- `restingOrders = 2048`
- `crossingOrders = 2048`
- `concurrency = 24`
- `batchSize = 64`（binary ingress 场景）

`restingOrders = 2048` 不是业务上限。它只是稳定 benchmark 口径：用 2048 笔预挂卖单形成足够深的可成交盘口，再用 2048 笔 crossing 买单完全吃掉该盘口。2048 是 2 的幂，便于固定批次、重复运行和观察队列/复制水位。

Binary HA committed 容量行使用 `32768/32768` 订单窗口，便于更稳定地观察复制确认收敛。

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
- 单备生产默认建议 `GRPC`；多备 quorum 拓扑应使用 committed benchmark 和 chaos 验证作为容量依据。
- `AERON` 是多备高性能可选项，但仍需要在目标硬件上做 soak、chaos 和网络尾延迟验证。

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
[Benchmark 基线](doc/operations/benchmark-baseline.md)。

| 模式                                                            | 推荐用途                             | accepted/s | trade events/s | committed/s |
| ------------------------------------------------------------- | -------------------------------- | ----------: | --------------: | ----------: |
| Core-only matcher                                             | 纯内存撮合主链                         | `24,855,862` | `N/A` | `N/A` |
| Single-node HTTP                                              | 通用业务服务                           | `6,532` | `6,532` | `6,532` |
| Single-node binary                                            | 高频单节点                            | `132,048` | `132,048` | `132,048` |
| External `1P1S` REST + `GRPC` local ack                       | REST 写入 + 单备复制，本地 WAL 返回         | `4,440` | `4,440` | `4,432` |
| External `1P1S` REST + `GRPC` committed ack                   | REST 写入 + 单备复制，等待 committed 返回   | `3,481` | `3,481` | `3,476` |
| External `1P1S` binary + `GRPC` replication committed         | 高频主入口 + 单备闭环真实 committed         | `374,104` | `374,104` | `368,309` |
| External `1P1S` binary + `AERON` replication committed        | 高频主入口 + 单备闭环真实 committed         | `244,833` | `244,833` | `241,824` |
| External `1P2S` binary + `GRPC` quorum replication committed  | 高频主入口 + 两备 quorum 闭环真实 committed | `348,068` | `348,068` | `214,529` |
| External `1P2S` binary + `AERON` quorum replication committed | 高频主入口 + 两备 quorum 闭环真实 committed | `342,143` | `342,143` | `130,568` |
| External `1P3S` binary + `GRPC` quorum replication committed  | 高频主入口 + 三备 quorum 闭环真实 committed | `339,801` | `339,801` | `192,615` |
| External `1P3S` binary + `AERON` quorum replication committed | 高频主入口 + 三备 quorum 闭环真实 committed | `164,802` | `164,802` | `37,618` |

### 推荐部署原则

- 项目部署边界是受控内网，不设计为直接公网暴露服务
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
  - `matcher.replicationMode`
  - `matcher.replicationTimeoutMillis`
  - `matcher.failoverMinStandbyReplicas`
  - `matcher.httpSubmitAckMode`
  - `matcher.httpSubmitBatchMaxOrders`
- 传输安全
  - `matcher.transportMtlsRequired`
  - `matcher.transportTlsReloadMillis`

Standalone 与 Spring Boot starter 共享 WAL 默认值：`SYNC_PER_COMMAND`、`walForceBatchSize=1`、`walForceMaxDelayMicros=0`。需要以吞吐优先的批量刷盘模式运行时，应显式配置这些参数并在故障演练中确认可接受的丢失窗口。

完整配置说明放在运维和架构文档中。

## 文档索引

- 入门与贡献：
  - [贡献指南](CONTRIBUTING.md)
  - [安全策略](SECURITY.md)
  - [行为准则](CODE_OF_CONDUCT.md)
- 架构：
  - [Shard 模型设计](doc/architecture/shard-model-design.md)
  - [多分片容量规划](doc/architecture/shard-capacity-planning.md)
  - [Spring Boot Starter 设计](doc/architecture/matcher-spring-boot-starter-design.md)
- 运维与压测：
  - [生产部署与容量规划](doc/operations/production-deployment-and-capacity.md)
  - [Benchmark 基线](doc/operations/benchmark-baseline.md)
  - [部署模式](doc/operations/deployment-modes.md)
  - [HA / Sharding 验证环境手册](doc/operations/ha-sharding-lab.md)
  - [HTTP 路由预算与可观测性](doc/operations/http-route-budget-observability.md)
  - [WAL / 复制 / 租约故障验证矩阵](doc/operations/wal-replication-lease-chaos-matrix.md)
  - [Shard 发布 Runbook](doc/operations/shard-rollout-runbook.md)
  - [Shard 发布检查清单](doc/operations/shard-rollout-checklist.md)
- `AERON_PREVIEW`、transport 切换窗口和预览验证请看 [HA / Sharding 验证环境手册](doc/operations/ha-sharding-lab.md)

## 最后说明

这个项目的目标是把单分片撮合节点做到可嵌入、可部署、可恢复、可复制、可观测。

如果你的目标是更大的总吞吐，正确做法是：

1. 先把单分片做实
2. 再按交易对或业务维度分片
3. 用真实 benchmark 验证总容量

不要把单个 HTTP 接口的吞吐，误认为整个交易系统的最终上限。
