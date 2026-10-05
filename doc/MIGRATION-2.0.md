# ull-matcher 2.0 集成与基线

本仓库**仅维护 2.0** 服务端与 **`matcher-sdk-java` 2.0** 客户端，不提供、不测试 1.x SDK 兼容路径。集成方请统一使用 2.0 坐标与协议（含 binary 32 字节握手、ingress API key）。

## 环境要求

- JDK **25**
- Maven **4**（请使用仓库内 `./mvnw`）
- Spring Boot 嵌入场景需要 **Spring Boot 4.x**

## 版本与坐标

- Maven 版本：**`2.0.0`**
- 可执行发布物：`matcher-server-dist/target/ull-matcher-server-dist-2.0.0.jar`
- Java 客户端：`io.github.ike:ull-matcher-sdk-java:2.0.0`

## Jackson 3

服务端、SDK、etcd 客户端使用 `tools.jackson.*`（Jackson 3）。

- SDK 公共 API 返回 `tools.jackson.databind.JsonNode`
- 上游若直接依赖 Jackson 2，需与 Jackson 3 对齐，或仅通过 SDK DTO/字符串解析

## 生产安全默认值

`matcher.serverMode=PROD` 时：

- 禁止 `matcher.walDurabilityMode=OS_BUFFERED`
- 禁止 `matcher.dataDir` 解析后的 WAL 目录包含 `target`、`build`、`out` 任意一级路径段
- HTTP/binary 绑定非 loopback 时必须配置 **`matcher.ingressApiKeys`**
- etcd 非 loopback 端点必须使用 **`https://`**（`EtcdConfig` 构造期校验，含 Spring Boot starter 路径）

## HTTP / Binary 鉴权

- HTTP：请求头 `X-Ull-Api-Key` 或 `Authorization: Bearer <key>`
- Binary：连接建立后先发送 **32 字节** UTF-8 密钥（不足补 0）；`MatcherBinaryClient` 会自动发送
- Spring：`ull.matcher.ingress-api-keys`、`ull.matcher.ingress-api-key-header`

### 健康检查

只有 **`GET /api/v1/runtime/live`** 免鉴权（仅 `{"status":"UP"}`）。  
`/api/v1/runtime/health`、`/runtime/state`、`/runtime/readiness` 需要 API key。

- k8s **livenessProbe** 建议使用 `/api/v1/runtime/live`
- **readinessProbe** 若使用 `/runtime/readiness`，需在 probe 上配置 `X-Ull-Api-Key`

密钥超过 32 UTF-8 字节会在 `IngressAuthConfig` 构造期被拒绝。

### Binary 连接限额

| 系统属性 | 默认值 | 含义 |
| --- | --- | --- |
| `matcher.binaryIngressMaxConnections` | `4096` | 并发连接上限 |
| `matcher.binaryIngressHandshakeTimeoutMillis` | `5000` | 未完成握手的连接存活上限 |
| `matcher.binaryIngressIdleTimeoutMillis` | `300000` | 已鉴权连接空闲上限，`0` 表示不回收 |

长连接心跳间隔大于空闲超时时，请调大 `binaryIngressIdleTimeoutMillis`。

## 写路径 ack（默认与强 SLA）

与 [production-deployment-and-capacity.md](operations/production-deployment-and-capacity.md) 一致：

- **默认：** primary 本地 WAL 受理（HTTP `ack=local`；binary 默认帧类型 `1`）
- **可选强 SLA：** replication committed（HTTP `ack=committed` / batch；binary 帧类型 `3`）
- **容量规划：** 一律按 **replication committed throughput**，不以 accepted  alone 承诺 HA 写入上限

## 复制传输

- **生产默认：`GRPC`**
- **`AERON` / `AERON_PREVIEW`：** 开源高级能力；生产启用前须完成 `transport-compare`、长稳 soak 与回滚演练（见 [ha-sharding-lab.md](operations/ha-sharding-lab.md)）。当前无强制生产 rollout 计划时，文档与 CI 以 GRPC 为准即可。

## etcd TLS

- `-Dmatcher.etcdTlsTrustChain=` / `etcdTlsCertChain=` / `etcdTlsPrivateKey=`
- Spring：`ull.matcher.cluster.etcd-tls-*`

私钥须为**未加密 PKCS#8**（`-----BEGIN PRIVATE KEY-----`）：

```bash
openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem
```

`etcdTlsCertChain` 与 `etcdTlsPrivateKey` 须同时提供或同时省略。

## 集群名属性

推荐使用 **`matcher.cluster`**。`matcher.clusterName` 仍可读但会打印弃用告警，计划 3.0 移除。

## SDK

`MatcherClientConfig` 支持 `ingressApiKey` 与 `ingressApiKeyHeader`（默认 `X-Ull-Api-Key`）。

## 推荐集成步骤

1. `./mvnw --batch-mode -Pstyle-check verify`
2. `./mvnw -pl matcher-server-dist -am package`（部署）或依赖 `ull-matcher-sdk-java:2.0.0`（客户端）
3. Lab：`scripts/run-chaos-tests.sh cluster failover-smoke` 与 [benchmark-baseline.md](operations/benchmark-baseline.md)
4. 生产：ingress API keys、gRPC mTLS、etcd https、分片路由与 [cto-signoff-2.0.md](operations/cto-signoff-2.0.md) §4–§5
