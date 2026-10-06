# HTTP 与 Binary 并发（推荐顺序）

按 **接入层 → 背压 → 撮合/WAL** 分层规划；不要只调大 HTTP 线程或连接数。

## 0. HTTP(S) 栈原则（NIO / 虚拟线程 / 零拷贝边界）

| 角色 | 实现 | 说明 |
| --- | --- | --- |
| **服务端 ingress** | Undertow（XNIO NIO 多路复用） | 不在 REST 层再叠 Netty；IO 线程收包，`exchange.dispatch` 把 handler 放到共享虚拟线程池 |
| **服务端 handler** | `MatcherHttpExecutors` + budget | 阻塞等待 WAL/committed 在虚拟线程上挂起，不占满 Undertow worker |
| **出站客户端** | `ull-matcher-net` / `MatcherHttpTransport` | 底层 NIO；默认 **HTTP/2** 多路复用；`send`/`sendAsync` 回调走共享虚拟线程 `matcher-http-client-*` |
| **高频写** | Binary ingress | 定长帧 + DirectBuffer，才是零拷贝与万级 QPS 方向；JSON REST 必然有序列化拷贝 |
| **响应体** | `ResponseSender.send(ByteBuffer)` | metrics / receipt 等尽量 `byte[]`→`ByteBuffer` 一次发送，避免中间 `String` |

客户端版本：`-Dmatcher.httpClientVersion=HTTP_1_1`（压测对照 HTTP/1.1 时使用）。服务端仍用 Undertow HTTP/1.1 keep-alive；客户端 HTTP/2 对单 host 多并发读 API 更省连接。

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

**读/写** handler 均经 `exchange.dispatch(共享虚拟线程池, …)` 执行（`MatcherHttpExecutors`，线程名 `matcher-http-*`）；最后一个 `HttpApiServer` 关闭时回收。Undertow worker 不再在 `future.get` 或长 WAL 等待上被占满。写路径同样受 **route 超时** 约束（共享 `matcher-http-timeout-*` 调度线程）。

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

REST **写**与读一样经虚拟线程 dispatch，但在 handler 内仍会阻塞等待撮合/WAL；吞吐天花板见 §1，请用 binary 承载高频写。

## 3. 容量与观测

- 规划以 **replication committed throughput** 为准，见 [production-deployment-and-capacity.md](production-deployment-and-capacity.md)。
- Prometheus：`ull_matcher_http_route_overload_total`、`ull_matcher_http_executor_queue_*`（虚拟线程模式下 queue 指标为 0，以 route/global overload 为准）。

## 4. 不建议

- 仅把 `httpMaxConcurrentRequests` 调到 10000 而不开 binary、不压测 committed 与 p99。
- 在生产关闭 HTTP budget（无 Semaphore）—— 会把过载转成 GC、WAL 尾延迟和 match loop 饥饿。
