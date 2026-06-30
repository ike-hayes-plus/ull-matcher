# HA / 分片实验环境手册

## 目标

本手册用于在本地或实验环境中快速验证以下能力：

- `1 primary + N standbys`
- 任意 `shardKey` 的控制面分片隔离
- WAL / 复制 / lease / readiness 基本闭环
- HTTP 路由预算与过载保护
- submission 查询面与复制确认闭环

本实验环境**不是**性能基准环境。

性能验证仍建议：

- matcher 节点直接跑在宿主机
- 使用 `taskset` / `numactl`
- 本地 NVMe
- 独占 CPU / NUMA

---

## 1. 组件组成

容器化实验环境负责基础设施：

- ZooKeeper
- etcd
- Toxiproxy
- Prometheus

matcher 节点本身仍建议直接跑在宿主机。

默认端口规划：

| 组件 | node-a | node-b | node-c |
| --- | --- | --- | --- |
| HTTP | `8080` | `8081` | `8082` |
| gRPC 复制/控制面 | `9190` | `9191` | `9192` |
| Aeron | `15090` | `15091` | `15092` |

基础设施端口：

- ZooKeeper：`2181`、`2182`、`2183`
- etcd：`2379`、`2381`、`2383`
- Toxiproxy：`8474`
- Prometheus：`19091`

启动基础设施：

```bash
./scripts/chaos/lab.sh up
```

停止：

```bash
./scripts/chaos/lab.sh down
```

一键验证基础设施：

```bash
./scripts/chaos/lab.sh validate
```

启动三节点主备实验集群：

```bash
./scripts/chaos/cluster.sh up
```

停止三节点实验集群：

```bash
./scripts/chaos/cluster.sh down
```

一键验证三节点状态：

```bash
./scripts/chaos/cluster.sh validate
```

执行主节点故障切换烟雾测试：

```bash
./scripts/chaos/cluster.sh failover-smoke
```

输出文件：

- `target/chaos-lab/validation-report.json`
- `target/chaos-lab/transport-rollout-report.json`
- `target/chaos-lab/chaos-summary.json`
- `target/failover-smoke/failover-smoke-report.json`
- `target/failover-smoke/transport-change-runbook.md`
- `target/failover-smoke/chaos-summary.json`
- `target/transport-compare/*/benchmark-report.json`
- `target/transport-compare/transport-comparison-summary.json`
- `target/transport-compare/*/crossing-benchmark-report.json`

---

## 2. 启动三节点主备实验

下面示例使用同一个 `shardKey`，形成 `1 primary + 2 standbys`：

- node-a
- node-b
- node-c

### node-a

```bash
java \
  -Dmatcher.serverMode=DEV \
  -Dmatcher.nodeId=node-a \
  -Dmatcher.symbolId=1 \
  -Dmatcher.shardKey=merchant:42 \
  -Dmatcher.dataDir=./data/node-a \
  -Dmatcher.httpPort=8080 \
  -Dmatcher.grpcPort=9190 \
  -Dmatcher.zkConnect=127.0.0.1:2181 \
  -Dmatcher.cluster=merchant-lab \
  -Dmatcher.advertisedHost=127.0.0.1 \
  io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain
```

### node-b

```bash
java \
  -Dmatcher.serverMode=DEV \
  -Dmatcher.nodeId=node-b \
  -Dmatcher.symbolId=1 \
  -Dmatcher.shardKey=merchant:42 \
  -Dmatcher.dataDir=./data/node-b \
  -Dmatcher.httpPort=8081 \
  -Dmatcher.grpcPort=9191 \
  -Dmatcher.zkConnect=127.0.0.1:2181 \
  -Dmatcher.cluster=merchant-lab \
  -Dmatcher.advertisedHost=127.0.0.1 \
  io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain
```

### node-c

```bash
java \
  -Dmatcher.serverMode=DEV \
  -Dmatcher.nodeId=node-c \
  -Dmatcher.symbolId=1 \
  -Dmatcher.shardKey=merchant:42 \
  -Dmatcher.dataDir=./data/node-c \
  -Dmatcher.httpPort=8082 \
  -Dmatcher.grpcPort=9192 \
  -Dmatcher.zkConnect=127.0.0.1:2181 \
  -Dmatcher.cluster=merchant-lab \
  -Dmatcher.advertisedHost=127.0.0.1 \
  io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain
```

更推荐直接使用一键脚本：

```bash
./scripts/chaos/cluster.sh up
```

如果要启用 Aeron 权威复制：

```bash
REPLICATION_TRANSPORT=AERON ./scripts/chaos/cluster.sh up
```

如果要做 gRPC 与 Aeron 预览对比：

```bash
REPLICATION_TRANSPORT=AERON_PREVIEW ./scripts/chaos/cluster.sh up
```

如果要演练“模式切换窗口”，建议显式带上：

```bash
-Dmatcher.allowTransportChange=true
-Dmatcher.transportChangeWindowId=lab-switch-001
```

节点级滚动切换建议直接使用单节点脚本：

```bash
./scripts/lab/stop-node.sh node-b
ALLOW_TRANSPORT_CHANGE=true TRANSPORT_CHANGE_WINDOW_ID=lab-switch-001 \
REPLICATION_TRANSPORT=AERON ./scripts/lab/start-node.sh node-b 8081 9191 15091
```

Java 21 下该模式需要：

```bash
--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

---

## 2.1 提交确认与查询

业务提交接口遵循“本地持久化回执 + 复制确认查询”的协议：

- `POST /api/v1/orders`
- `POST /api/v1/orders/cancel`
- `GET /api/v1/submissions/{submissionId}`
- `GET /api/v1/submissions/by-idempotency?idempotencyKey=...`

建议：

- 调用方显式传入 `Idempotency-Key`
- 把同步 HTTP 回执视为“当前节点已接收并落入本地持久化流程”
- 把 submission 查询结果视为“是否已满足复制确认策略”
- 把订单状态查询或事件流视为最终成交/撤单结果

---

## 3. 分片隔离实验

再启动另一组节点，但用不同 `shardKey`，例如：

- `merchant:99`

目标：

- 两个分片可以共享基础设施
- 但 discovery、复制、failover 不应互相干扰

这正是 `shardKey` 的控制面隔离语义。

---

## 4. 一键状态采集与校验

基础设施 + 节点一起验证：

```bash
./scripts/chaos/lab.sh validate target/chaos-lab \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081 \
  http://127.0.0.1:8082
```

该脚本会：

1. 检查 ZooKeeper / etcd / Toxiproxy / Prometheus
2. 抓取各节点的：
   - `/api/v1/runtime/health`
   - `/api/v1/runtime/readiness`
   - `/metrics`
3. 校验：
   - 最多一个 `PRIMARY`
   - 最多一个 `clientTrafficReady=true`
   - `PRIMARY` 与可接业务流量节点一致

`cluster failover-smoke` 的判定标准：

1. 基线阶段只有一个 `PRIMARY`
2. kill 当前 primary
3. 幸存节点中出现新的 `clientTrafficReady=true`
4. 新 primary URL 与旧 primary URL 不同

---

## 5. 推荐观测

### 节点 readiness

- `serviceReady`
- `clientTrafficReady`
- `promotionReady`
- `catchUpInProgress`
- `snapshotSyncRequired`

### 复制与 HA

- `ull_matcher_ha_received_lag`
- `ull_matcher_ha_durable_lag`
- `ull_matcher_ha_applied_lag`
- `ull_matcher_ha_snapshot_lag`
- `ull_matcher_ha_tick_failures_total`

### HTTP 路由预算

- `ull_matcher_http_global_saturation`
- `ull_matcher_http_route_saturation{route="write"}`
- `ull_matcher_http_route_overload_total{route="write"}`
- `ull_matcher_http_route_timeout_total{route="read"}`

### 提交与撮合

- `ull_matcher_submission_pending`
- `ull_matcher_submission_committed`
- `ull_matcher_submission_failed`
- `ull_matcher_submission_retrying`
- `ull_matcher_trade_events_total`
- `ull_matcher_order_events_total`
- `ull_matcher_loop_processed_commands`

其中：

- `submission_*` 用于判断提交协议是否在复制确认阶段堆积
- `trade_events_total` 用于判断真实成交输出能力
- `loop_processed_commands` 用于判断主链处理能力

---

## 5.1 传输压测报告解读

`transport-benchmark` 报告里的吞吐档位使用 `并发度x每轮订单数x连续轮数` 记法。

例如 `24x960x2` 表示：

1. 24 个并发 worker 同时发单。
2. 每轮发 960 笔订单。
3. 连续执行 2 轮，总共 1920 笔订单。

重点看四组指标：

1. `acceptedOrdersPerSecond`：订单进入系统的能力。
2. `processedCommandsPerSecond`：主链实际处理命令的能力。
3. `tradeEventsPerSecond`：真实成交事件输出能力。
4. `matchedOrderSidesPerSecond`：参与成交的订单边数/秒，按每笔成交折算买卖双方各一边。

判断方法：

1. `acceptedOrdersPerSecond` 低，优先检查 HTTP 预算、提交链、复制确认或背压策略。
2. `acceptedOrdersPerSecond` 高但 `tradeEventsPerSecond` 低，优先检查撮合与订单簿处理能力。
3. 两者都高，说明入口和撮合主链都处于健康区间。

---

## 6. 常见演练

### 演练 1：主节点 kill -9

```bash
PRIMARY_PID=<pid> ./scripts/run-chaos-tests.sh env kill-primary
```

观察：

- standby 是否接管
- 是否只产生一个 primary

如果你只是想快速验证切主链路，直接执行上文已经列出的 `cluster failover-smoke` 即可。

### 演练 2：ZooKeeper 断联

```bash
./scripts/run-chaos-tests.sh env zk-disconnect
./scripts/run-chaos-tests.sh env zk-reconnect
```

观察：

- 原 primary 是否自我 fencing
- standby 是否误 promote

### 演练 3：复制延迟

```bash
LATENCY_MS=250 ./scripts/run-chaos-tests.sh env grpc-delay
./scripts/run-chaos-tests.sh env netem-reset
```

观察：

- lag 是否上升
- readiness 是否正确降级

---

## 7. 注意事项

1. 本实验环境用于功能验证，不用于真实性能压测
2. 真正的低延迟压测仍建议宿主机直跑
3. `shardKey` 决定控制面隔离，`symbolId` 决定撮合内核市场标识
