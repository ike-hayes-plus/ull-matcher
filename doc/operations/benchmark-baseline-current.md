# Benchmark 基线

本文档定义 README 和容量规划使用的 benchmark 基线。

这些数字是本地参考机器上的基线，不是服务等级承诺。吞吐会受到 CPU、磁盘、内核调度、JVM 版本、网络路径、WAL durability、复制拓扑和 benchmark 参数影响。

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

`restingOrders = 2048` 不是容量上限。它用于形成固定且可完全成交的盘口：先放入 2048 笔 resting sell orders，再用 2048 笔 crossing buy orders 吃掉盘口。2048 是 2 的幂，足够覆盖队列、批次、WAL force 和复制水位，又能让本地压测保持可重复。

## 压测结果

| 场景 | 入口 | 复制 | 口径 | Accepted orders/s | Trade events/s | Committed submissions/s | Catch-up | p99 延迟 |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| Core-only matcher | 直接内存调用 | 无 | 只测 `UltraLowLatencyMatcher.onCommand(...)`，不含 HTTP、WAL、HA、IPC | `24,855,862.10` | `N/A` | `N/A` | `N/A` | `0.13 us` |
| 本地持久化服务路径 | JVM 内服务调用 | 无 | 撮合主链 + 本地 WAL + 事件分发 | `171,211.81` | `171,211.81` | `N/A` | `N/A` | `N/A` |
| Single-node HTTP | REST | 无 | REST 下单入口 + 本地 WAL | `6,531.96` | `6,531.96` | `6,531.96` | `N/A` | `86.08 ms` |
| Single-node binary | Binary ingress | 无 | Binary ingress + 本地 WAL | `132,048.10` | `132,048.10` | `132,048.10` | `N/A` | `0.21 ms` |
| External `1P1S` REST + `GRPC` | REST | `GRPC` | 外部 REST 写入 + 单备复制 | `4,249.67` | `4,249.67` | `4,239.82` | `0.0011 s` | `11.90 ms` |
| External `1P1S` REST + `AERON` | REST | `AERON` | 外部 REST 写入 + 单备复制 | `4,157.51` | `4,157.51` | `4,147.76` | `0.0012 s` | `13.66 ms` |
| External `1P1S` binary + `GRPC` | Binary ingress | `GRPC` | 外部 binary 写入 + 单备复制 | `118,535.39` | `118,535.39` | `107,297.23` | `0.0018 s` | `0.35 ms` |
| External `1P1S` binary + `AERON` | Binary ingress | `AERON` | 外部 binary 写入 + 单备复制 | `103,706.48` | `103,706.48` | `92,479.28` | `0.0024 s` | `0.40 ms` |
| External `1P2S` binary + `GRPC` quorum | Binary ingress | `GRPC` | 外部 binary 写入 + 两备 quorum | `90,381.18` | `90,381.18` | `83,693.46` | `0.0018 s` | `0.57 ms` |
| External `1P2S` binary + `AERON` quorum | Binary ingress | `AERON` | 外部 binary 写入 + 两备 quorum | `119,573.78` | `119,573.78` | `71,270.73` | `0.0116 s` | `0.24 ms` |
| External `1P3S` binary + `GRPC` quorum | Binary ingress | `GRPC` | 外部 binary 写入 + 三备 quorum | `60,545.52` | `60,545.52` | `57,556.45` | `0.0018 s` | `0.66 ms` |
| External `1P3S` binary + `AERON` quorum | Binary ingress | `AERON` | 外部 binary 写入 + 三备 quorum | `82,831.98` | `82,831.98` | `14,390.44` | `0.1176 s` | `0.24 ms` |

Core-only matcher 是纯内存撮合基线，用来证明核心数据结构、价格队列和撮合逻辑的上限。它不经过服务入口、WAL、复制或提交确认，因此不参与 committed 容量规划。

REST HA 的 REST 只表示外部调用撮合服务的方式。主备复制不走 HTTP，只走配置的复制传输。

容量规划应使用 `Committed submissions/s`，不要只看 `Accepted orders/s`。

## 解读

- 撮合核心不是主要容量瓶颈。
- Binary ingress 是高频写入主入口。
- REST 适合管理、查询、后台操作和低频普通业务流量。
- REST HA 的吞吐低于 binary HA，主要来自 HTTP 解析、请求响应、WAL、复制确认和外部进程调度成本。
- 生产容量规划以 replication committed throughput 为准。
- `GRPC` 是保守默认复制传输。
- `AERON` 可用于高性能场景，但应在目标拓扑下使用同样的 benchmark、soak 和 chaos 口径验证。

## 复现

生成 benchmark 报告到 `target/`：

```bash
mvn --batch-mode --no-transfer-progress -DskipTests install
scripts/lab/run-binary-ingress-benchmark.sh \
  --mode replication-commit \
  --transport GRPC \
  --standbys 1 \
  --standby-commit-mode any \
  --report target/current/binary-commit/grpc-1p1s-current.json
```

归档通过校验的报告：

```bash
scripts/ops/archive-current-benchmarks.sh <archive-name>
```

归档脚本会拒绝非法 JSON 和 `success=false` 报告，并校验 Java 与 Python benchmark 报告的 schema metadata。
