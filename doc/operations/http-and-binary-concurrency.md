# HTTP 与 Binary 并发（推荐顺序）

按 **接入层 → 背压 → 撮合/WAL** 分层规划；不要只调大 HTTP 线程或连接数。

## 1. 写流量：Binary ingress（首选）

- 高频下单、撤单、committed ack 走 **binary**（定长帧 + DirectBuffer），不要指望 REST 过万 QPS。
- 基线对比见 [benchmark-baseline.md](benchmark-baseline.md)（单节点 REST ~5.7k orders/s vs binary ~7.7万+ orders/s 量级）。
- 部署：每个节点在 `NODES` 中保留 **非 `-` 的 binaryPort**（见 [cluster.conf.example](../../scripts/deploy/cluster.conf.example)）。

可调 JVM / 环境：

| 项 | 默认 | 说明 |
| --- | ---: | --- |
| `matcher.binaryIngressMaxConnections` | 4096 | 同时在线连接上限 |
| `matcher.binaryIngressHandshakeTimeoutMillis` | 5000 | 未鉴权连接超时 |
| `matcher.binaryIngressIdleTimeoutMillis` | 300000 | 空闲连接回收 |

## 2. HTTP 调度：Undertow IO → 虚拟线程 + 保留 budget

**读/写** handler 均经 `exchange.dispatch(虚拟线程池, …)` 执行，Undertow worker 不再在 `future.get` 或长 WAL 等待上被占满。写路径同样受 **route 超时** 约束（与读一致）。

**背压不变**：全局 / 读·写·管理路由 / 端点 Semaphore 仍生效（503 overload），保护 ring、WAL 与内存。

| 项 | 默认 | 说明 |
| --- | --- | --- |
| （默认） | 虚拟线程 | 每读请求一条虚拟线程 |
| `-Dmatcher.httpPlatformReadExecutor=true` | 关闭 VT | 恢复旧版有界平台线程池（排障用） |
| `matcher.httpMaxConcurrentRequests` | **2048** | 全局在途 HTTP 上限 |
| `matcher.httpReadMaxConcurrentRequests` | **1024** | 读路由 budget |
| `matcher.httpWriteMaxConcurrentRequests` | **1024** | 写路由 budget |
| `matcher.httpSubmitEndpointMaxConcurrentRequests` | **512** | POST 下单端点 |
| `matcher.httpShardWriteMaxConcurrentRequests` | **1024** | 分片写准入（与写 budget 对齐） |
| `matcher.httpWorkerThreads` | **max(32, 2×CPU)** | Undertow worker/IO 规模基线 |

拐点扫频：`scripts/lab/run-http-concurrency-sweep.sh`（内置 `SingleNodeServerCrossingBenchmark`）。

REST **写**仍在 Undertow worker 上阻塞等待撮合/WAL；提高写并发请回到 §1。

## 3. 容量与观测

- 规划以 **replication committed throughput** 为准，见 [production-deployment-and-capacity.md](production-deployment-and-capacity.md)。
- Prometheus：`ull_matcher_http_route_overload_total`、`ull_matcher_http_executor_queue_*`（虚拟线程模式下 queue 指标为 0，以 route/global overload 为准）。

## 4. 不建议

- 仅把 `httpMaxConcurrentRequests` 调到 10000 而不开 binary、不压测 committed 与 p99。
- 在生产关闭 HTTP budget（无 Semaphore）—— 会把过载转成 GC、WAL 尾延迟和 match loop 饥饿。
