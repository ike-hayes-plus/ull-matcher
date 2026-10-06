# ull-matcher 3.0 集成基线

本仓库以 **3.0** 为唯一产品基线：单分片撮合节点 + 可选多分片编排。不提供历史版本迁移路径。

## 坐标

- Maven 父版本：**`3.0.0`**
- 可执行发布物：`matcher-server-dist/target/ull-matcher-server-dist-3.0.0.jar`
- Java 客户端：`io.github.ike:ull-matcher-sdk-java:3.0.0`
- 出站 HTTP 传输（etcd / SDK 共用）：`io.github.ike:ull-matcher-net:3.0.0`（`MatcherHttpTransport`）

## 快速集成

1. `./mvnw -Pstyle-check verify`
2. 部署：`./mvnw -pl matcher-server-dist -am package`；或依赖 SDK 3.0.0
3. 配置：`matcher.cluster`、ingress API keys、复制传输 TLS（`matcher.transportTls*`）、etcd https（PROD）
4. 多分片：`-Dmatcher.orchestratorEnabled=true` + etcd；路由查询见 [shard-orchestration-3.0.md](architecture/shard-orchestration-3.0.md)

## 生产检查

- [production-deployment-and-capacity.md](operations/production-deployment-and-capacity.md)
- [security-boundary.md](operations/security-boundary.md)
- [cto-signoff-3.0.md](operations/cto-signoff-3.0.md)
