# 当前稳定压测基线

本文档用于归档当前仓库状态下，适合在开源 README 中引用的稳定压测事实源。

以下数据全部来自当前代码状态，只能视为当前基线，不应被表述成永久承诺。

原始 JSON 报告由仓库内 benchmark 脚本生成，并通过 `scripts/ops/archive-current-benchmarks.sh` 归档。开源文档不把 `target/current/**` 当成对外可见路径。

## 压测机器

- Apple M4 Pro
- 12 逻辑 CPU
- 24 GiB 内存
- 本地 SSD

## 基线场景

### 纯撮合主链

- 口径：
  - 单 JVM
  - 只测 `UltraLowLatencyMatcher.onCommand(...)`
  - 不包含 HTTP
  - 不包含 WAL
  - 不包含 HA
  - 不包含 IPC

当前基线：
- accepted orders/s：`6,992,019.06`
- p99 延迟：`0.46 µs`

### 单 JVM 本地持久化主链

- 口径：
  - 单 JVM
  - 包含撮合主链、本地 WAL 和事件分发
  - 不包含外部网络边界
  - 不包含 HA

当前基线：
- accepted orders/s：`48,977.35`

### 单节点 Binary ingress

- 口径：
  - binary ingress
  - 本地单进程
  - 不包含 HA

当前基线：
- accepted orders/s：`20,075.79`

### 外部 Binary + 复制确认闭环

当前基线：

| 场景 | Accepted orders/s | Replication committed/s | Catch-up |
|---|---:|---:|---:|
| External `1P1S` binary + `GRPC` | `21,648.10` | `19,935.02` | `0.0081 s` |
| External `1P1S` binary + `AERON` | `17,359.70` | `16,136.80` | `0.0089 s` |
| External `1P2S` binary + `GRPC` quorum | `17,739.98` | `16,495.97` | `0.0087 s` |
| External `1P2S` binary + `AERON` quorum | `12,319.45` | `10,404.45` | `0.0306 s` |
| External `1P3S` binary + `GRPC` quorum | `13,697.46` | `12,887.54` | `0.0094 s` |
| External `1P3S` binary + `AERON` quorum | `27,829.30` | `21,109.76` | `0.0234 s` |

## 解释

- 当前主瓶颈不是 matcher core。
- binary ingress 是当前高频接单的正确主入口。
- 当前生产主问题是 HA 持久化复制链里的 `accepted -> committed` 折损。
- `GRPC` 仍然是当前保守默认值，因为单备 committed 曲线最稳定。
- `AERON` 是高性能可选实现，但多备曲线仍然需要更长时间的 soak 和 chaos 证据，才能替代保守默认值。

## 最终 Soak / Chaos 证据

最终发布前，已对当前 `GRPC` 三节点拓扑跑过一轮 soak / failover 收尾验证。

验证范围：

- 60 秒稳定采样
- `serviceReady`、`clientTrafficReady` 持续观测
- failover smoke
- 复制传输策略一致性验证

当前结论：

- soak 窗口内 `allAccepting=true`
- soak 窗口内 `allServiceReady=true`
- soak 窗口内 `allClientTrafficReady=true`
- failover smoke 汇总结果为 `success=true`

说明：

- 该轮证据用于证明当前 `GRPC` 默认拓扑具备稳定的 readiness 与 failover 基线。
- 该轮证据不等同于长时间生产 soak，也不替代更长期的 chaos 演练。
