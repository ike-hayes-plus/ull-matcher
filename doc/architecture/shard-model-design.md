# 分片模型设计

## 目标

`ull-matcher` 的分片模型不预设固定业务类型。

系统只要求：

- 每个分片在任意时刻只有一个 active primary
- 每个分片可以有多个 standby
- 分片内保持独立的复制、接管、恢复和观测边界

分片键由 `shardKey` 抽象表达，允许上层业务自由定义。

---

## 1. 分片抽象

### 1.1 控制面分片键

`shardKey` 是控制面唯一识别一个分片的主键。

它可以表示任意业务维度，例如：

- `merchant:10001`
- `country:ae`
- `tenant:vip`
- `region:mena`
- `market:spot-btcusdt`
- `book:global`

系统不要求 `shardKey` 遵循固定枚举，也不要求预定义分片类型。

### 1.2 撮合内核分片

当前撮合内核仍然是单交易对、单线程拥有状态，核心撮合对象由 `symbolId` 表达。

因此当前实现是“两层模型”：

- `symbolId`：撮合内核层的单市场标识
- `shardKey`：控制面层的主备、复制、接管、发现标识

这意味着：

- 一个 `shardKey` 当前对应一个内核实例
- 上层可以按任意业务维度规划分片
- 控制面不把分片类型写死在代码里

---

## 2. 支持的拓扑

### 2.1 不分片

- 一个全局 `shardKey`
- 一个 primary
- 多个 standby

### 2.2 按任意业务维度分片

例如：

- 按商户
- 按区域
- 按国家
- 按租户
- 按业务线

每个 `shardKey` 都独立拥有：

- lease / fencing
- readiness
- replication
- snapshot / replay
- active owner 选举结果

### 2.3 一主多备

当前模型支持：

- `1 primary`
- `N standbys`

standby 数量不被写死为 1。

控制面会基于：

- 复制追平状态
- snapshot lag
- fencing lease
- 探测结果

选择最合适的 standby 接管。

---

## 3. 当前代码实现对齐情况

### 已对齐

1. `MatcherServerConfig` 增加了 `shardKey`
2. `MatcherClusterConfig` 增加了 `shardKey`
3. 节点注册元数据包含 `shardKey`
4. `MatcherClusterSupervisor` 在 discovery 列表中按 `shardKey` 过滤
5. Spring Boot Starter 已暴露 `ull.matcher.shard-key`

### 当前边界

当前实现仍然保持：

- 一个 `matcher-server` 进程只承载一个撮合内核实例
- 一个内核实例当前只负责一个 `symbolId`

也就是说：

- 控制面分片是抽象和通用的
- 内核层仍然是单市场内核

这符合当前项目的低延迟设计目标。

---

## 4. 为什么不把分片类型写死

如果代码内直接写死：

- `merchant shard`
- `country shard`
- `tenant shard`

会带来两个问题：

1. 控制面和业务域耦合
2. 新分片维度需要改框架代码

因此当前设计坚持：

- 框架只理解 `shardKey`
- 业务自己决定 `shardKey` 的语义

这样扩展成本最低，也更适合开源项目。

---

## 5. 设计建议

### 推荐

- 使用稳定、可读、不可歧义的 `shardKey`
- 让 `shardKey` 成为上游配置和部署编排的一等字段
- 在监控、日志、注册发现中统一透出 `shardKey`

### 不推荐

- 把 `cluster`、`symbolId`、`shardKey` 混用成同一个概念
- 让一个节点在没有明确资源隔离的情况下同时承载多个活跃分片
- 在控制面代码里硬编码特定业务分片类型

---

## 6. 后续演进方向

如果未来要继续扩展，可以沿着两条线推进：

1. **多分片编排层**
   - 一个宿主机统一管理多个 `matcher-server` 进程或内核实例
   - 做更高阶的资源调度

2. **多市场内核层**
   - 一个进程承载多个撮合内核实例
   - 但仍然保持每个内核单线程拥有自己的状态

当前项目主线仍建议优先保持：

- 一个进程
- 一个活跃撮合内核
- 一个 `shardKey`

这样更符合低延迟和可恢复性的目标。
