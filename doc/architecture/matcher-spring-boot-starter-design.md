# Matcher Spring Boot Starter 设计

## 目标

`ull-matcher-spring-boot-starter` 的目标不是把 `matcher-server` 变成 Spring Cloud 服务，也不是引入 Nacos、Gateway 或配置中心依赖，而是给已有 Spring Boot 应用提供一个轻量嵌入装配入口。

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
- Nacos / Spring Cloud 服务发现适配
- Spring Cloud 配置中心适配

这些能力应放在外部 Gateway / Ingress / 平台配置层，不进入 matcher 核心项目。

## 推荐边界

```text
Gateway / Ingress / Sidecar
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

## Starter 范围

starter 提供：

- `ull.matcher.*` 基础属性
- 独立 `MatcherServerConfig` Bean
- `ull.matcher.auto-start=true` 时自动启动 `MatcherServerApp`
- `MatcherClusterConfig` 属性绑定
- `ServerSecurityConfig` 属性绑定
- `TtlCancelConfig` 属性绑定
- WAL durability 相关属性绑定
- ZooKeeper / etcd `LeaseStore` 与 `NodeRegistry` 条件化装配

starter 故意不做：

- 平台级控制器
- Spring MVC / WebFlux API 包装
- Nacos / Spring Cloud 服务发现适配
- Spring Cloud 配置中心适配

原因是这些能力会把主项目重新拉回“重 Spring 服务框架”形态，污染撮合节点边界。

## 运行边界

starter 只负责把 matcher 节点嵌入 Spring Boot 进程。平台能力由外部组件承载：

- Gateway / Ingress 负责 TLS 终止、鉴权、租户路由和平台限流
- ZooKeeper / etcd 负责 lease 与节点发现
- Actuator、配置中心和业务控制器由宿主 Spring Boot 应用自行装配

starter 不承载统一业务网关控制器。
