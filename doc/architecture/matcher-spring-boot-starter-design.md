# Matcher Spring Boot Starter 设计

## 目标

`ull-matcher-spring-boot-starter` 的目标不是把 `matcher-server` 变成 Spring Cloud Gateway，而是给 Spring Boot / Spring Cloud 生态提供一个轻量装配入口。

它负责：

- `MatcherServerConfig` 的属性映射
- `MatcherServerApp` 的可选自动启动
- 允许上游通过 Spring Bean 覆盖：
  - `MatcherClusterConfig`
  - `ServerSecurityConfig`
  - `TtlCancelConfig`

它不负责：

- API Gateway
- 鉴权
- 路由聚合
- 平台级限流
- Spring Cloud Gateway 过滤链

这些能力应放在外部 Spring Cloud Gateway / Ingress 层。

## 推荐边界

```text
Spring Cloud Gateway / Ingress
            |
            v
  ull-matcher-spring-boot-starter
            |
            v
       matcher-server
            |
            v
 core / storage / runtime / ha / grpc
```

## 当前 starter 范围

当前 starter 提供：

- `ull.matcher.*` 基础属性
- 独立 `MatcherServerConfig` Bean
- `ull.matcher.auto-start=true` 时自动启动 `MatcherServerApp`
- `MatcherClusterConfig` 属性绑定
- `ServerSecurityConfig` 属性绑定
- `TtlCancelConfig` 属性绑定
- WAL durability 相关属性绑定

当前 starter 故意不做：

- 平台级控制器
- Spring MVC / WebFlux API 包装

原因是这些能力会把主项目重新拉回“重 Spring 服务框架”路线，污染撮合节点边界。

## 后续扩展建议

如需继续扩展，优先顺序应为：

1. 健康指标桥接到 Actuator
2. Nacos / ZooKeeper 条件化装配样例
3. 与 Spring Cloud 配置中心做外部集成
4. 更细粒度的 Spring Boot configuration metadata

不建议在 starter 中新增统一业务网关控制器。
