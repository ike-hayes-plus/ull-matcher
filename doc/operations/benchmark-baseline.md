# Benchmark 基线

本文档定义 README 和容量规划使用的 benchmark 基线。数字来自参考机器上的可复现验证，不是服务等级承诺。吞吐会受到 CPU、磁盘、内核调度、JVM 版本、网络路径、WAL durability、复制拓扑和 benchmark 参数影响。

## 参考机器

- Apple M4 Pro
- 12 逻辑 CPU
- 24 GiB 内存
- 本地 SSD
- **JDK Temurin 25.0.3**（2.0 运行时与本文「压测结果」口径）

正式 JSON 报告目录：`target/benchmark/current/`（`scripts/ops/run-ha-benchmark-suite.sh`，Temurin 25 / `.sdkmanrc`）。

## 标准场景

除非单项 benchmark 另有说明，crossing 场景使用：

- `restingOrders = 2048`
- `crossingOrders = 2048`
- `concurrency = 24`
- binary ingress 场景使用 `batchSize = 64`

`restingOrders = 2048` 不是容量上限。它用于形成固定且可完全成交的盘口：先放入 2048 笔 resting sell orders，再用 2048 笔 crossing buy orders 吃掉盘口。2048 是 2 的幂，便于固定批次、重复运行和观察队列、WAL force、复制水位。

Binary HA committed 基线使用更长的 `restingOrders = 32768`、`crossingOrders = 32768` 窗口。该窗口更适合观察 replication committed 收敛、quorum ack、WAL force 和多 standby fan-out，在容量规划上比短窗口更稳定。

`1P1S` binary + `GRPC` 另有百万级扩展窗口验证，用来确认默认容量附近的大挂单簿可以完成真实 committed 链路；该结果不是容量规划主基线。

## 压测结果

下表数值与 `target/benchmark/current/` 中最新 `success=true` 报告一致。HA 主矩阵复现：

```bash
COOLDOWN_SECONDS=45 scripts/ops/run-ha-benchmark-suite.sh
```

| 场景 | 入口 | 复制 | 口径 | Result | Accepted orders/s | Trade events/s | Committed submissions/s | Catch-up | p99 延迟 |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| Core-only matcher | 直接内存调用 | 无 | 只测 `UltraLowLatencyMatcher.onCommand(...)`，不含 HTTP、WAL、HA、IPC | PASS | `32,464,634` | `N/A` | `N/A` | `N/A` | `0.08 us` |
| 本地持久化服务路径 | JVM 内服务调用 | 无 | 撮合主链 + 本地 WAL + 事件分发 | PASS | `123,770` | `123,770` | `N/A` | `N/A` | `N/A` |
| Single-node HTTP | REST | 无 | REST 下单入口 + 本地 WAL | PASS | `5,722` | `5,722` | `5,722` | `N/A` | `100.03 ms` |
| Single-node binary | Binary ingress | 无 | Binary ingress + 本地 WAL | PASS | `77,143` | `77,143` | `77,143` | `N/A` | `0.30 ms` |
| External `1P1S` REST + `GRPC` local ack | REST | `GRPC` | 外部 REST 写入 + 单备复制，写响应按本地 WAL 返回 | PASS | `3,375` | `3,375` | `3,369` | `0.001 s` | `14.81 ms` |
| External `1P1S` REST + `GRPC` committed ack | REST | `GRPC` | 外部 REST 写入 + 单备复制，写响应等待 replication committed | PASS | `3,403` | `3,403` | `3,397` | `0.001 s` | `19.50 ms` |
| External `1P1S` binary + `GRPC` any | Binary ingress | `GRPC` | 外部 binary 写入 + 单备复制，`zk/zk` 控制面，`32768/32768` 订单窗口 | PASS | `302,069` | `302,069` | `264,048` | `0.016 s` | `0.15 ms` |
| External `1P1S` binary + `AERON` any | Binary ingress | `AERON` | 外部 binary 写入 + 单备复制，`zk/zk` 控制面，`32768/32768` 订单窗口 | PASS | `270,880` | `270,880` | `210,889` | `0.034 s` | `0.16 ms` |
| External `1P2S` binary + `GRPC` quorum | Binary ingress | `GRPC` | 外部 binary 写入 + 两备 quorum，`zk/zk` 控制面，`32768/32768` 订单窗口，frame `1` | PASS | `210,000` | `210,000` | `199,000` | `0.009 s` | `0.24 ms` |
| External `1P2S` binary + `GRPC` quorum committed frame | Binary ingress | `GRPC` | 同上，frame `3` 回包前等待 replication committed | PASS | `93,000` | `93,000` | `92,000` | `0.002 s` | `0.64 ms` |
| External `1P2S` binary + `AERON` quorum | Binary ingress | `AERON` | 外部 binary 写入 + 两备 quorum，`zk/zk` 控制面，`32768/32768` 订单窗口 | PASS | `324,817` | `324,817` | `78,769` | `0.315 s` | `0.18 ms` |
| External `1P3S` binary + `GRPC` quorum | Binary ingress | `GRPC` | 外部 binary 写入 + 三备 quorum，`zk/zk` 控制面，`32768/32768` 订单窗口 | PASS | `194,859` | `194,859` | `182,880` | `0.011 s` | `0.26 ms` |
| External `1P3S` binary + `AERON` quorum | Binary ingress | `AERON` | 外部 binary 写入 + 三备 quorum，`zk/zk` 控制面，`32768/32768` 订单窗口 | PASS | `297,155` | `297,155` | `43,807` | `0.638 s` | `0.16 ms` |

Core-only matcher 是纯内存撮合基线，用来证明核心数据结构、价格队列和撮合逻辑的上限。它不经过服务入口、WAL、复制或提交确认，因此不参与 committed 容量规划。

REST HA 的 REST 只表示外部调用撮合服务的方式。主备复制不走 HTTP，只走配置的复制传输。

容量规划应使用 `Committed submissions/s`，不要只看 `Accepted orders/s`。

`1P3S` 的 quorum 只需 3 个备库中的 2 个确认。本次 `GRPC` `1P3S` 窗口内 `allStandbysDurableCommands=0`（quorum committed 仍可达 `182,880/s`），不能当作三备都已追上的容量。`AERON` `1P3S` 全备 durable 约 `39,615/s`，与 quorum committed `43,807/s` 仍有差距。

## 百万级窗口验证

该验证在同一参考机器上执行（Temurin 25，报告 `target/benchmark/current/retest/grpc-1p1s-1m.json`）：

```bash
scripts/lab/run-binary-ingress-benchmark.sh \
  --mode replication-commit \
  --transport GRPC \
  --standbys 1 \
  --standby-commit-mode any \
  --resting-orders 1000000 \
  --crossing-orders 1000000 \
  --concurrency 24 \
  --batch-size 64 \
  --report target/benchmark/binary-commit/grpc-1p1s-1m-current.json
```

结果：

| 场景 | 入口 | 复制 | 口径 | Result | Accepted orders/s | Trade events/s | Committed submissions/s | Catch-up | p99 延迟 |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| External `1P1S` binary + `GRPC` any, 1M window | Binary ingress | `GRPC` | 外部 binary 写入 + 单备复制，`zk/zk` 控制面，`1,000,000/1,000,000` 订单窗口，另有 `256` warmup 买单不计入窗口 | PASS | `443,568` | `443,568` | `437,182` | `0.033 s` | `0.13 ms` |

该窗口先预挂 `1,000,000` 笔卖单，再统计 `1,000,000` 笔 crossing 买单。报告中 `acceptedCommands=1,000,000`、`tradeEvents=1,000,000`、`replicationCommittedSubmissions=1,000,000`、`rejectedCommands=0`。由于默认 `expectedLiveOrders` 和 `orderPoolSize` 都是 `1,048,576`，生产单分片如果可能长期接近或超过百万活跃挂单，应显式调大撮合容量配置。

## HTTP 性能边界

HTTP 入口定位为管理面、查询、补单和普通业务接入。高频撮合入口应使用 binary ingress。

单节点 HTTP / binary 与本地持久化路径的 Temurin 25 数字见上表。REST HA 的 catch-up 与 committed/s 见 `target/benchmark/current/` 中 REST 报告；REST 主成本在 HTTP/JSON/同步请求模型，而不是撮合核心。

HTTP/2 多路复用和 HTTP keep-alive 能降低连接层开销，但不会消除服务端每个 request exchange 的业务执行成本。`POST /api/v1/orders/batch` 通过一个 HTTP 请求承载多笔新单，减少请求数、worker 调度次数和响应序列化次数，适合普通业务批量写入，单次请求默认最多 `1024` 笔，可通过 `matcher.httpSubmitBatchMaxOrders` 调整。要把入口吞吐推近服务内核上限，应使用批量 REST、长连接专用客户端或 binary ingress。

REST committed 对比基线使用本地 `1P2S`、`GRPC`、`1024` 笔 crossing 订单、`concurrency=16`：

| REST 提交模式 | batch size | accepted/s | committed/s | p99 延迟 | catch-up |
| --- | ---: | ---: | ---: | ---: | ---: |
| REST committed single | `1` | `2,876` | `2,863` | `11.81 ms` | `0.001 s` |
| REST committed batch | `32` | `33,447` | `32,149` | `0.48 ms` | `0.001 s` |

REST `1P2S` 报告见 `target/benchmark/current/rest-1p2s-single.json` 与 `rest-1p2s-batch32.json`。GRPC 复制路径使用每备库有序长连接流、零 accumulation 微等待；`POST /api/v1/orders/batch` 在 `ack=committed` 时先将整批命令入队再统一等待复制确认，使复制与 JSON/HTTP 解析重叠；单笔仍走 JSON 快路径与零 Map 回执。

该数据说明 REST 单笔链路仍受每请求同步模型限制；批量 REST 在 committed 模式下可接近复制/WAL 带宽上限，面向普通业务批量写入，不替代高频 binary ingress。

Binary ingress 默认帧类型 `1` 在响应中只确认本地 WAL 受理；复制 committed 由客户端或基准脚本通过 health/watermark 观测。帧类型 `3`（`NEW_ORDER_BATCH_COMMITTED`）与 REST `ack=committed` batch 同形：整帧先入队，再统一等待 replication committed 后回包（第四字段 `reserved=1` 表示已 committed）。

`1P2S` + `GRPC` + quorum + `32768/32768` 复测（frame `1`，报告 `target/benchmark/current/grpc-1p2s-binary-frame1.json`）：accepted **~210k/s**，committed **~199k/s**，p99 **~0.24 ms**。同场景 frame `3`（`--committed-frame`，报告 `grpc-1p2s-binary-committed-frame.json`）：accepted **~93k/s**，committed **~92k/s**，p99 **~0.64 ms**——吞吐计入回包前等待复制，catch-up 近 0；同批内顺序 await（共享 deadline），避免 frame-3 热路径上 per-batch 虚拟线程池开销。

```bash
scripts/lab/run-binary-ingress-benchmark.sh \
  --mode replication-commit --transport GRPC --standbys 2 --standby-commit-mode quorum \
  --resting-orders 32768 --crossing-orders 32768 --concurrency 24 --batch-size 64 \
  --report target/benchmark/current/grpc-1p2s-binary-frame1.json
# wire-level committed ack:
#   ... --committed-frame --report target/benchmark/current/grpc-1p2s-binary-committed-frame.json
```

HA 报告 floor（非 CI 每跑 cluster，但 lab 产出报告后可 gate）：

```bash
scripts/ops/check-ha-benchmark-floor.sh
# 或 REPORT_DIR=target/benchmark/current scripts/ops/check-ha-benchmark-floor.sh --require-all
```

阈值见 `doc/operations/benchmark-ha-ci-floor.json`（frame `1` accepted ≥ 120k/s，committed ≥ 110k/s）。

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
| `1P1S` binary + `GRPC` any | ZooKeeper lease + ZooKeeper discovery | PASS | `302,069` | `264,048` | `0.016 s` | `0.15 ms` |
| `1P2S` binary + `GRPC` quorum | ZooKeeper lease + ZooKeeper discovery | PASS | `210,000` | `199,000` | `0.009 s` | `0.24 ms` |
| `1P3S` binary + `GRPC` quorum | ZooKeeper lease + ZooKeeper discovery | PASS | `194,859` | `182,880` | `0.011 s` | `0.26 ms` |
| `1P1S` binary + `AERON` any | ZooKeeper lease + ZooKeeper discovery | PASS | `270,880` | `210,889` | `0.034 s` | `0.16 ms` |
| `1P2S` binary + `AERON` quorum | ZooKeeper lease + ZooKeeper discovery | PASS | `324,817` | `78,769` | `0.315 s` | `0.18 ms` |
| `1P3S` binary + `AERON` quorum | ZooKeeper lease + ZooKeeper discovery | PASS | `297,155` | `43,807` | `0.638 s` | `0.16 ms` |
| `1P1S` binary + `GRPC` any | etcd lease + etcd discovery | PASS | `352,798` | `232,741` | `0.048 s` | `0.17 ms` |
| `1P2S` binary + `GRPC` quorum | etcd lease + etcd discovery | PASS | `359,075` | `186,704` | `0.084 s` | `0.16 ms` |
| `1P3S` binary + `GRPC` quorum | etcd lease + etcd discovery | PASS | `452,565` | `195,617` | `0.095 s` | `0.13 ms` |
| `1P1S` binary + `AERON` any | etcd lease + etcd discovery | PASS | `487,647` | `330,515` | `0.032 s` | `0.12 ms` |
| `1P2S` binary + `AERON` quorum | etcd lease + etcd discovery | PASS | `347,350` | `94,150` | `0.254 s` | `0.23 ms` |
| `1P3S` binary + `AERON` quorum | etcd lease + etcd discovery | PASS | `270,054` | `31,457` | `0.920 s` | `0.54 ms` |

该矩阵说明控制面实现没有进入订单热路径；同一控制面下的差异主要来自复制传输、standby 数量和 quorum ack 收敛成本。参考机器上，`AERON` 单备 catch-up 更短；两备和三备 quorum 的 catch-up 明显长于 `GRPC`。`GRPC` `1P3S` 的 quorum committed 更高，但有一个备库没有追上全量 durable。etcd 上的 `GRPC` `1P3S` 同样如此：quorum committed 是 `195,617/s`，测量窗口里全备 durable 命令数是 0。生产选型应优先考虑团队运维经验、现有基础设施、故障演练成熟度和监控体系；把 `AERON` 作为多备默认传输前，应在目标硬件上完成单独归因和长稳验证。

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

参考机器上的三节点 qualification 结果：

| 控制面 | Soak 窗口 | baseline | soak | kill primary | old primary restore | stop one member | restore member | Result |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- |
| ZooKeeper lease + ZooKeeper discovery | `20 s` | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| etcd lease + etcd discovery | `20 s` | PASS | PASS | PASS | PASS | PASS | PASS | PASS |

该验证不替代生产长稳压测。生产准入应在目标硬件和真实网络上延长 soak 窗口，并覆盖控制面网络分区、磁盘延迟、进程重启和旧主恢复。

## 复现

生成 benchmark 报告：

```bash
./mvnw --batch-mode --no-transfer-progress -DskipTests install
scripts/lab/run-binary-ingress-benchmark.sh \
  --mode replication-commit \
  --transport GRPC \
  --standbys 1 \
  --standby-commit-mode any \
  --report target/benchmark/binary-commit/grpc-1p1s.json
```

REST batch benchmark 示例：

```bash
python3 scripts/bench/replication-commit-benchmark.py \
  --base-url http://127.0.0.1:8080 \
  --report target/benchmark/http-ha/rest-batch-local.json \
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
  --report target/benchmark/http-ha/rest-single-1024.json

scripts/lab/run-rest-commit-benchmark.sh \
  --base-url http://127.0.0.1:8080 \
  --standby-base-url http://127.0.0.1:8081 \
  --wait-for-ready-seconds 30 \
  --http-submit-mode batch \
  --batch-size 32 \
  --report target/benchmark/http-ha/rest-batch-1024.json
```

校验生成报告不低于本文档基线：

```bash
scripts/ops/validate-benchmark-baseline.py
```

## CI 回归门禁

上面的完整基线需要 HA lab，跑不进每次 PR。CI 里跑的是不需要集群的子集：

```bash
scripts/ops/run-benchmark-regression.sh
```

它执行 core-only 和 embed-journaled-core 两个场景，并用
[benchmark-ci-floor.json](benchmark-ci-floor.json) 里的下限校验结果。

这两套门禁的定位不同，不要混用：

| | `validate-benchmark-baseline.py` | `check-benchmark-regression.py` |
| --- | --- | --- |
| 跑在哪 | 参考机 / HA lab，手动 | 每次 PR，GitHub runner |
| 比什么 | 本文档表格里的真实基线 | `benchmark-ci-floor.json` 的保守下限 |
| 容差 | 吞吐 0.5%、延迟 5% | 下限比基线低一个数量级 |
| 能发现 | 几个百分点的性能漂移 | 热路径上多了锁 / 分配 / 系统调用导致的塌方 |

共享 runner 的抖动远大于几个百分点，所以 CI 门禁只做塌方检测。想看漂移，
必须在参考机上跑完整基线。

### WAL 元数据 fsync

2.0 在段文件创建时增加了一次 `FileChannel.force(true)`。这是**正确性修复**，不是性能优化：
`truncate()` 只改页缓存里的文件长度，此前数据页可能已经 msync 落盘而长度没有，
崩溃后会留下一个被截短的段，丢掉已经 ack 过的命令。

代价是每个段文件一次 fsync，不是每条命令一次 —— 提交路径上的 `force()` 仍然只有
`MappedByteBuffer.force()`。

A/B 复测（同机、`SYNC_PER_COMMAND`、`restingOrders=2048`、`crossingOrders=2048`，
各跑 3 次，`acceptedOrdersPerSecond`）：

| | 第 1 次 | 第 2 次 | 第 3 次 |
| --- | ---: | ---: | ---: |
| 加 `force(true)` | `122,968` | `125,641` | `114,874` |
| 不加 | `102,345` | `99,301` | `116,032` |

两组区间重叠，运行间抖动大于两组的差值，**测不出可区分的开销**。这符合预期：
额外的 fsync 只在段文件创建时发生一次，会被整个段的命令数摊薄。

该 A/B 在同一参考机、Temurin 25、`SYNC_PER_COMMAND` 下完成，与单节点 embed 场景口径一致。
