# Benchmark 基线

本文档定义 README 和容量规划使用的 benchmark 基线。数字来自参考机器上的可复现实验，不是服务等级承诺。吞吐会受到 CPU、磁盘、内核调度、JVM 版本、网络路径、WAL durability、复制拓扑和 benchmark 参数影响。

## 参考机器

- Apple M4 Pro
- 12 逻辑 CPU
- 24 GiB 内存
- 本地 SSD
- JDK 21

## 标准场景

除非单项 benchmark 另有说明，crossing 场景使用：

- `restingOrders = 2048`
- `crossingOrders = 2048`
- `concurrency = 24`
- binary ingress 场景使用 `batchSize = 64`

`restingOrders = 2048` 不是容量上限。它用于形成固定且可完全成交的盘口：先放入 2048 笔 resting sell orders，再用 2048 笔 crossing buy orders 吃掉盘口。2048 是 2 的幂，便于固定批次、重复运行和观察队列、WAL force、复制水位。

## 压测结果

| 场景 | 入口 | 复制 | 口径 | Result | Accepted orders/s | Trade events/s | Committed submissions/s | Catch-up | p99 延迟 |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| Core-only matcher | 直接内存调用 | 无 | 只测 `UltraLowLatencyMatcher.onCommand(...)`，不含 HTTP、WAL、HA、IPC | PASS | `24,855,862.10` | `N/A` | `N/A` | `N/A` | `0.13 us` |
| 本地持久化服务路径 | JVM 内服务调用 | 无 | 撮合主链 + 本地 WAL + 事件分发 | PASS | `171,211.81` | `171,211.81` | `N/A` | `N/A` | `N/A` |
| Single-node HTTP | REST | 无 | REST 下单入口 + 本地 WAL | PASS | `6,531.96` | `6,531.96` | `6,531.96` | `N/A` | `86.08 ms` |
| Single-node binary | Binary ingress | 无 | Binary ingress + 本地 WAL | PASS | `132,048.10` | `132,048.10` | `132,048.10` | `N/A` | `0.21 ms` |
| External `1P1S` REST + `GRPC` local ack | REST | `GRPC` | 外部 REST 写入 + 单备复制，写响应按本地 WAL 返回 | PASS | `4,440.25` | `4,440.25` | `4,432.39` | `0.0008 s` | `11.76 ms` |
| External `1P1S` REST + `GRPC` committed ack | REST | `GRPC` | 外部 REST 写入 + 单备复制，写响应等待 replication committed | PASS | `3,480.57` | `3,480.57` | `3,475.70` | `0.0008 s` | `11.64 ms` |
| External `1P1S` binary + `GRPC` any | Binary ingress | `GRPC` | 外部 binary 写入 + 单备复制 | PASS | `140,245.95` | `140,245.95` | `123,799.21` | `0.0019 s` | `0.30 ms` |
| External `1P1S` binary + `AERON` any | Binary ingress | `AERON` | 外部 binary 写入 + 单备复制 | PASS | `78,915.05` | `78,915.05` | `71,821.85` | `0.0026 s` | `0.65 ms` |
| External `1P2S` binary + `GRPC` quorum | Binary ingress | `GRPC` | 外部 binary 写入 + 两备 quorum | PASS | `85,661.70` | `85,661.70` | `40,706.60` | `0.0264 s` | `0.58 ms` |
| External `1P2S` binary + `AERON` quorum | Binary ingress | `AERON` | 外部 binary 写入 + 两备 quorum | PASS | `126,513.47` | `126,513.47` | `40,952.42` | `0.0338 s` | `0.19 ms` |
| External `1P3S` binary + `GRPC` quorum | Binary ingress | `GRPC` | 外部 binary 写入 + 三备 quorum | PASS | `90,155.39` | `90,155.39` | `54,688.93` | `0.0147 s` | `0.47 ms` |
| External `1P3S` binary + `AERON` quorum | Binary ingress | `AERON` | 外部 binary 写入 + 三备 quorum | PASS | `120,963.94` | `120,963.94` | `19,463.93` | `0.0883 s` | `0.25 ms` |

Core-only matcher 是纯内存撮合基线，用来证明核心数据结构、价格队列和撮合逻辑的上限。它不经过服务入口、WAL、复制或提交确认，因此不参与 committed 容量规划。

REST HA 的 REST 只表示外部调用撮合服务的方式。主备复制不走 HTTP，只走配置的复制传输。

容量规划应使用 `Committed submissions/s`，不要只看 `Accepted orders/s`。

## HTTP 性能边界

HTTP 入口定位为管理面、查询、补单和普通业务接入。高频撮合入口应使用 binary ingress。

参考机器上，Single-node HTTP 为 `6,531.96` orders/s，Single-node binary 为 `132,048.10` orders/s，本地持久化服务路径为 `171,211.81` orders/s。REST HA local ack 与 committed ack 的 catch-up 都约为 `0.0008 s`，说明该场景的主要成本不在撮合核心或主备复制，而在单笔 HTTP 请求、JSON 编解码、同步请求/响应、幂等提交跟踪和客户端连接模型。

HTTP/2 多路复用和 HTTP keep-alive 能降低连接层开销，但不会消除服务端每个 request exchange 的业务执行成本。`POST /api/v1/orders/batch` 通过一个 HTTP 请求承载多笔新单，减少请求数、worker 调度次数和响应序列化次数，适合普通业务批量写入，单次请求默认最多 `1024` 笔，可通过 `matcher.httpSubmitBatchMaxOrders` 调整。要把入口吞吐推近服务内核上限，应使用批量 REST、长连接专用客户端或 binary ingress。

REST committed 对比基线使用本地 `1P2S`、`GRPC`、`1024` 笔 crossing 订单、`concurrency=16`：

| REST 提交模式 | batch size | accepted/s | committed/s | p99 延迟 | catch-up |
| --- | ---: | ---: | ---: | ---: | ---: |
| REST committed single | `1` | `3,081.43` | `3,072.18` | `9.83 ms` | `0.0010 s` |
| REST committed batch | `32` | `7,655.93` | `7,593.97` | `2.75 ms` | `0.0012 s` |

该数据说明 REST 单笔链路仍受请求/响应模型限制；批量 REST 能明显摊薄 HTTP 与 JSON 开销，但它面向普通业务批量写入，不替代高频 binary ingress。

## 指标口径

`Accepted orders/s` 与 `Committed submissions/s` 使用不同时间窗口：

- `Accepted orders/s = accepted / submitWindowSeconds`
- `Committed submissions/s = committed / totalWindowSeconds`
- `totalWindowSeconds = submitWindowSeconds + commitCatchupSeconds`

在多备 quorum 场景下，committed/s 包含提交结束后等待复制确认追平的时间，因此通常低于 accepted/s。这不是丢单，也不表示只有一部分订单 committed。只要 `accepted == committed` 且 benchmark `success=true`，说明压测窗口内提交链最终收敛。

## HTTP 压测模式

REST benchmark 支持两种提交模式：

- `single`：每笔订单一个 `POST /api/v1/orders`
- `batch`：每批订单一个 `POST /api/v1/orders/batch`

`batch` 模式用于衡量应用层复用对 REST 入口的收益。报告字段 `httpSubmitMode` 和 `batchSize` 标识提交方式。

## 控制面对比

控制面支持两种生产组合：

- `zk/zk`：ZooKeeper lease + ZooKeeper discovery
- `etcd/etcd`：etcd lease + etcd discovery

本地 lab 使用三节点 ZooKeeper ensemble 和三 endpoint etcd quorum。控制面不应进入订单热路径；以下数据用于验证控制面实现没有明显拖累主备复制链路。

| 场景 | 控制面 | Result | Accepted orders/s | Committed submissions/s | Catch-up | p99 延迟 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `1P1S` binary + `GRPC` any | ZooKeeper lease + ZooKeeper discovery | PASS | `99,959.94` | `50,813.45` | `0.0198 s` | `0.44 ms` |
| `1P1S` binary + `GRPC` any | etcd lease + etcd discovery | PASS | `126,186.72` | `115,236.37` | `0.0015 s` | `0.29 ms` |
| `1P3S` binary + `GRPC` quorum | 3-node ZooKeeper lease + ZooKeeper discovery | PASS | `67,016.62` | `30,778.81` | `0.0360 s` | `0.89 ms` |
| `1P3S` binary + `GRPC` quorum | 3-endpoint etcd lease + etcd discovery | PASS | `72,457.85` | `32,953.68` | `0.0339 s` | `0.69 ms` |

三 endpoint etcd 在参考机器上略高于三节点 ZooKeeper ensemble，但差距不是数量级差异。生产选型应优先考虑团队运维经验、现有基础设施、故障演练成熟度和监控体系。

## 控制面准入验证

控制面 qualification 入口：

```bash
SOAK_SECONDS=60 CONTROL_PLANE=etcd \
  ./scripts/chaos/cluster.sh control-plane-qualification

SOAK_SECONDS=60 CONTROL_PLANE=zk \
  ./scripts/chaos/cluster.sh control-plane-qualification
```

准入项：

| 场景 | 期望结果 |
| --- | --- |
| baseline | 形成单 writable primary，探针订单成功 |
| soak | 观察窗口内持续单主，探针订单持续成功 |
| kill primary | standby 接管，client traffic 迁移到新 primary |
| old primary restore | 原 primary 恢复后不重新接写 |
| stop one etcd member | etcd quorum 可用，matcher 继续单主可写 |
| restore etcd member | etcd 成员恢复后保持单 primary |
| stop one ZooKeeper member | ZooKeeper quorum 可用，matcher 继续单主可写 |
| restore ZooKeeper member | ZooKeeper 成员恢复后保持单 primary |

该验证不替代生产长稳压测。生产准入应在目标硬件和真实网络上延长 soak 窗口，并覆盖控制面网络分区、磁盘延迟、进程重启和旧主恢复。

## 复现

生成 benchmark 报告到 `target/current/`：

```bash
mvn --batch-mode --no-transfer-progress -DskipTests install
scripts/lab/run-binary-ingress-benchmark.sh \
  --mode replication-commit \
  --transport GRPC \
  --standbys 1 \
  --standby-commit-mode any \
  --report target/current/binary-commit/grpc-1p1s-current.json
```

REST batch benchmark 示例：

```bash
python3 scripts/bench/replication-commit-benchmark.py \
  --base-url http://127.0.0.1:8080 \
  --report target/current/http-ha/rest-batch-local.json \
  --ack-mode local \
  --http-submit-mode batch \
  --batch-size 32
```

连接已有集群时使用封装入口：

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

归档通过校验的报告：

```bash
scripts/ops/archive-current-benchmarks.sh <archive-name>
```

归档脚本会拒绝非法 JSON 和 `success=false` 报告，并校验 Java 与 Python benchmark 报告的 schema metadata。

校验生成报告不低于本文档基线：

```bash
scripts/ops/validate-benchmark-baseline.py
```
