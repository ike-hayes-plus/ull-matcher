# ull-matcher 3.0 CTO Sign-off（一页纸）

**版本：** 3.0.0  
**范围：** 单分片撮合节点 + 可选 etcd 多分片编排  
**审查：** [cto-review-3.0.md](cto-review-3.0.md)（**100/100**）

## Go 条件

- [x] `./mvnw -Pstyle-check verify` 全绿
- [x] HTTP：Undertow NIO + 共享虚拟线程 dispatch + budget
- [x] 出站 HTTP：`ull-matcher-net` / `MatcherHttpTransport`
- [x] 编排：register / lookup HTTP / SDK（`matcher.orchestratorEnabled`）
- [x] 文档：[INTEGRATION.md](../INTEGRATION.md)、生产部署、安全边界

## 客户端

`io.github.ike:ull-matcher-sdk-java:3.0.0` + binary ingress；编排 `MatcherOrchestratorClient`。

## 签字

| 角色 | 结论 | 日期 |
| --- | --- | --- |
| 架构 / CTO | **Go** — 100/100 | 2026-10-06 |
