# HTTP 路由预算观测与告警模板

## 目标

`matcher-server` 的 HTTP 入口不是统一网关，但需要具备节点自保护能力。

当前节点已经暴露：

- 全局并发预算
- 写 / 读 / 管理三类路由预算
- 关键 endpoint 独立并发硬预算
- 路由级超时
- 过载拒绝统计

本文件给出推荐观测和告警模板。

---

## 关键指标

### 全局入口

| 指标 | 说明 |
| --- | --- |
| `ull_matcher_http_global_overload_total` | 因全局并发预算耗尽被拒绝的累计次数 |
| `ull_matcher_http_global_inflight` | 当前正在处理的 HTTP 请求数 |
| `ull_matcher_http_global_saturation` | 当前全局请求预算使用率，`1.0` 表示全局已打满 |

### 路由级

| 指标 | 说明 |
| --- | --- |
| `ull_matcher_http_route_requests_total{route="write|read|admin"}` | 各类路由累计请求量 |
| `ull_matcher_http_route_overload_total{route="..."}` | 各类路由因预算耗尽被拒绝的累计次数 |
| `ull_matcher_http_route_timeout_total{route="..."}` | 各类路由因超时被终止的累计次数 |
| `ull_matcher_http_route_inflight{route="..."}` | 当前各类路由正在占用的并发槽位 |
| `ull_matcher_http_route_saturation{route="..."}` | 当前各类路由预算使用率，`1.0` 表示该类路由已打满 |

### Endpoint 级

| 指标 | 说明 |
| --- | --- |
| `ull_matcher_http_endpoint_requests_total{endpoint="..."}` | 单个接口累计请求量 |
| `ull_matcher_http_endpoint_overload_total{endpoint="..."}` | 单个接口因自身预算或全局预算被拒绝的累计次数 |
| `ull_matcher_http_endpoint_timeouts_total{endpoint="..."}` | 单个接口因超时被终止的累计次数 |
| `ull_matcher_http_endpoint_failures_total{endpoint="..."}` | 单个接口执行失败累计次数 |
| `ull_matcher_http_endpoint_inflight{endpoint="...",route="..."}` | 当前单个接口占用中的并发槽位 |
| `ull_matcher_http_endpoint_max_inflight{endpoint="...",route="..."}` | 进程启动以来该接口观察到的最大并发占用值 |
| `ull_matcher_http_endpoint_budget_saturation{endpoint="...",route="..."}` | 单个接口独立预算使用率，`1.0` 表示 endpoint 自己打满 |
| `ull_matcher_http_endpoint_route_saturation_share{endpoint="...",route="..."}` | 单个接口当前占用占其所属 route budget 的比例 |
| `ull_matcher_http_endpoint_duration_ms_sum{endpoint="..."}` | 单个接口累计处理耗时总和 |
| `ull_matcher_http_endpoint_duration_ms_max{endpoint="..."}` | 单个接口观察到的最大处理耗时 |
| `ull_matcher_http_endpoint_duration_bucket{endpoint="...",le="..."}` | 单个接口处理耗时分桶统计 |
| `ull_matcher_http_shard_write_overload_total{shard="..."}` | 当前分片写预算被拒绝的累计次数 |
| `ull_matcher_http_shard_write_inflight{shard="..."}` | 当前分片写预算正在占用的并发槽位 |
| `ull_matcher_http_shard_write_saturation{shard="..."}` | 当前分片写预算使用率 |
| `ull_matcher_http_shard_write_rate_limited_total{shard="..."}` | 当前分片写流量因速率桶耗尽被拒绝的累计次数 |
| `ull_matcher_http_shard_write_rate_limit_per_second{shard="..."}` | 当前分片写速率上限配置 |
| `ull_matcher_http_tenant_write_overload_total` | 租户级写预算被拒绝的累计次数 |
| `ull_matcher_http_tenant_write_rate_limited_total` | 租户级写流量因速率桶耗尽被拒绝的累计次数 |
| `ull_matcher_http_tenant_write_rate_limit_per_second` | 单租户基础写速率上限配置 |
| `ull_matcher_http_tenant_write_default_weight` | 默认租户权重配置 |
| `ull_matcher_http_tenant_write_active_entries` | 当前存在并发写流量的租户预算条目数 |
| `ull_matcher_http_tenant_write_anonymous_total` | 因缺少租户标识而按匿名模式进入写流控的累计次数 |

---

## 推荐看板

### 面板 1：写路由健康

- `rate(ull_matcher_http_route_requests_total{route="write"}[1m])`
- `rate(ull_matcher_http_route_overload_total{route="write"}[1m])`
- `rate(ull_matcher_http_route_timeout_total{route="write"}[1m])`
- `ull_matcher_http_route_saturation{route="write"}`

关注点：

- 写流量是否被读流量或管理操作挤占
- 写路由是否长期在 `0.8` 以上运行
- 写路由是否持续出现过载拒绝

### 面板 2：读路由健康

- `rate(ull_matcher_http_route_requests_total{route="read"}[1m])`
- `rate(ull_matcher_http_route_overload_total{route="read"}[1m])`
- `rate(ull_matcher_http_route_timeout_total{route="read"}[1m])`
- `ull_matcher_http_route_saturation{route="read"}`

关注点：

- 观测流量是否影响业务写路由
- readiness / health / metrics 查询是否异常放大

### 面板 3：管理路由健康

- `rate(ull_matcher_http_route_requests_total{route="admin"}[5m])`
- `rate(ull_matcher_http_route_overload_total{route="admin"}[5m])`
- `ull_matcher_http_route_saturation{route="admin"}`

关注点：

- 快照等管理操作是否占满入口

### 面板 4：关键 endpoint 预算

- `ull_matcher_http_endpoint_budget_saturation{endpoint="submit_order"}`
- `ull_matcher_http_endpoint_budget_saturation{endpoint="cancel_order"}`
- `ull_matcher_http_endpoint_budget_saturation{endpoint="runtime_readiness"}`
- `ull_matcher_http_endpoint_budget_saturation{endpoint="metrics"}`
- `rate(ull_matcher_http_endpoint_overload_total{endpoint="submit_order"}[1m])`
- `rate(ull_matcher_http_endpoint_overload_total{endpoint="metrics"}[1m])`

关注点：

- 是否某个关键接口先于整个 route budget 被打满
- 是否高频 metrics/readiness 抓取消耗掉独立 endpoint 预算
- 是否批量撤单挤占了 submit_order 的写入口

### 面板 5：分片 / 租户写预算

- `ull_matcher_http_shard_write_saturation{shard="..."}`
- `increase(ull_matcher_http_shard_write_overload_total{shard="..."}[5m])`
- `increase(ull_matcher_http_tenant_write_overload_total[5m])`
- `ull_matcher_http_tenant_write_active_entries`

关注点：

- 当前分片是否在 route budget 之外又触发了更高层的写入口保护
- 是否单个热点租户持续挤占写入口
- 调用方是否稳定透传了租户 Header

---

## Prometheus 告警模板

```yaml
groups:
  - name: ull-matcher-http-route-budget
    rules:
      - alert: UllMatcherGlobalHttpOverload
        expr: increase(ull_matcher_http_global_overload_total[5m]) > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "matcher-server 全局 HTTP 预算开始拒绝请求"
          description: "过去 5 分钟发生了全局 HTTP budget overload。应检查上游限流、节点容量和慢请求。"

      - alert: UllMatcherWriteRouteSaturationHigh
        expr: ull_matcher_http_route_saturation{route=\"write\"} > 0.8
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "写路由预算接近打满"
          description: "write route saturation 持续高于 0.8，建议检查上游写流量、节点 CPU 和 WAL 延迟。"

      - alert: UllMatcherWriteRouteOverload
        expr: increase(ull_matcher_http_route_overload_total{route=\"write\"}[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "写路由已发生过载拒绝"
          description: "过去 5 分钟 write 路由出现 overload，说明核心下单入口已开始拒绝业务流量。"

      - alert: UllMatcherReadRouteTimeoutSpike
        expr: increase(ull_matcher_http_route_timeout_total{route=\"read\"}[10m]) > 20
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "读路由超时显著上升"
          description: "查询/健康接口超时增加，建议检查观测流量、线程池和下游依赖。"

      - alert: UllMatcherAdminRouteSaturationHigh
        expr: ull_matcher_http_route_saturation{route=\"admin\"} > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "管理路由预算接近打满"
          description: "快照或其他管理操作可能过于频繁，建议限制执行频率。"

      - alert: UllMatcherSubmitEndpointOverload
        expr: increase(ull_matcher_http_endpoint_overload_total{endpoint=\"submit_order\"}[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "下单入口已触发 endpoint 级过载拒绝"
          description: "submit_order 自身预算或全局预算已开始拒绝请求，应检查上游限流和节点容量。"

      - alert: UllMatcherMetricsEndpointBudgetHigh
        expr: ull_matcher_http_endpoint_budget_saturation{endpoint=\"metrics\"} > 0.8
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "metrics 抓取接口预算接近打满"
          description: "metrics 接口正在占用过多读预算，建议降低抓取频率或扩容读预算。"
```

---

## 告警联动建议

### 写路由过载

优先排查：

1. 上游网关是否已做限流
2. `ull_matcher_http_global_saturation`
3. `ull_matcher_http_route_saturation{route="write"}`
4. WAL 持久化模式是否过于保守
5. 磁盘 `fsync` 抖动是否增大

如果 `submit_order` 的 endpoint budget 先打满，还要继续看：

6. `ull_matcher_http_endpoint_budget_saturation{endpoint="submit_order"}`
7. 是否需要单独提高 `matcher.httpSubmitEndpointMaxConcurrentRequests`

如果是更高层的分片/租户预算触发过载，还要继续看：

8. `ull_matcher_http_shard_write_saturation{shard="..."}`
9. `increase(ull_matcher_http_tenant_write_overload_total[5m])`
10. 调用方是否正确透传了 `matcher.httpTenantAdmissionHeader`

### 读路由过载

优先排查：

1. metrics / readiness 是否被高频抓取
2. 查询接口调用方是否异常放量
3. 是否需要进一步压缩读路由预算
4. `metrics` 或 `runtime_readiness` 的 endpoint budget 是否持续接近 1.0

### 管理路由过载

优先排查：

1. snapshot 触发频率
2. 是否有人为批量管理动作
3. 是否需要把管理操作迁到更低峰时段

### 分片 / 租户写预算过载

优先排查：

1. 当前 `shardKey` 是否已经成为热点分片
2. 是否单个租户持续放量
3. 是否需要为热点租户单独拆分 `shardKey`
4. 是否需要单独提高 `matcher.httpShardWriteMaxConcurrentRequests`
5. 是否需要启用或调高 `matcher.httpTenantWriteMaxConcurrentRequests`

---

## 推荐阈值起点

| 场景 | 建议阈值 |
| --- | --- |
| 写路由饱和度预警 | `> 0.8 for 3m` |
| 写路由过载告警 | `increase(overload_total[5m]) > 0` |
| 读路由超时预警 | `increase(timeout_total[10m]) > 20` |
| 管理路由饱和度预警 | `> 0.8 for 5m` |
| 全局预算过载预警 | `increase(global_overload_total[5m]) > 0` |

这些阈值只是起点，应该按你的实际峰值流量和分片模型调优。
